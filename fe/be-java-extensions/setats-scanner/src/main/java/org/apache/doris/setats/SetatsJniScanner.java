// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.setats;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;

import com.tencent.oceanus.predicate.Predicate;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.io.CloseableIterable;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.io.CloseableIterator;
import com.tencent.oceanus.shaded.org.apache.flink.table.data.RowData;
import com.tencent.oceanus.shaded.org.apache.flink.table.types.logical.LogicalType;
import com.tencent.oceanus.shaded.org.apache.flink.table.types.logical.TimestampType;
import com.tencent.oceanus.table.Table;
import com.tencent.oceanus.table.source.ReadBuilder;
import com.tencent.oceanus.table.source.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SetatsJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(SetatsJniScanner.class);
    private static final String HADOOP_CONF_PREFIX = "hadoop_conf.";
    private final Map<String, String> params;
    private final String setatsSplit;
    private final String setatsPredicate;
    private final SetatsColumnValue columnValue = new SetatsColumnValue();
    private final ClassLoader classLoader;
    private Table table;
    private CloseableIterable<RowData> reader;
    private CloseableIterator<RowData> recordIterator;
    private List<String> setatsAllFieldNames;
    private final Map<String, String> fsOptionsProps;
    private final PreExecutionAuthenticator preExecutionAuthenticator;

    public SetatsJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        if (LOG.isDebugEnabled()) {
            LOG.debug("params:{}", params);
        }
        this.params = params;
        String[] requiredFields = params.get("required_fields").split(",");
        String[] requiredTypes = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        this.setatsSplit = params.get("setats_split");
        this.setatsPredicate = params.get("setats_predicate");
        initTableInfo(columnTypes, requiredFields, batchSize);
        this.fsOptionsProps = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(HADOOP_CONF_PREFIX)) {
                fsOptionsProps.put(entry.getKey().substring(HADOOP_CONF_PREFIX.length()), entry.getValue());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get setats params {}: {}", entry.getKey(), entry.getValue());
            }
        }
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(fsOptionsProps);
    }

    @Override
    public void open() throws IOException {
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            preExecutionAuthenticator.execute(() -> {
                initTable();
                initReader();
                return null;
            });
            resetDatetimeV2Precision();
        } catch (Exception e) {
            LOG.warn("Failed to open setats_scanner: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void initReader() throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        readBuilder.withProjection(getProjected());
        readBuilder.withFilter(getPredicates());
        reader = readBuilder.newRead().createReaderAndRead(getSplit());
        recordIterator = reader.iterator();
    }

    private int[] getProjected() {
        return Arrays.stream(fields).mapToInt(setatsAllFieldNames::indexOf).toArray();
    }

    private Predicate getPredicates() {
        Predicate predicate = SetatsUtils.deserialize(setatsPredicate);
        if (LOG.isDebugEnabled()) {
            LOG.debug("predicates:{}", predicate);
        }
        return predicate;
    }

    private Split getSplit() {
        Split split = SetatsUtils.deserialize(setatsSplit);
        if (LOG.isDebugEnabled()) {
            LOG.debug("split:{}", split);
        }
        return split;
    }

    private void resetDatetimeV2Precision() {
        for (int i = 0; i < types.length; i++) {
            if (types[i].isDateTimeV2()) {
                // support precision > 6, but it has been reset as 6 in FE
                // try to get the right precision for datetimev2
                int index = setatsAllFieldNames.indexOf(fields[i]);
                if (index != -1) {
                    LogicalType dataType = table.rowType().getTypeAt(index);
                    if (dataType instanceof TimestampType) {
                        types[i].setPrecision(((TimestampType) dataType).getPrecision());
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (recordIterator != null) {
            recordIterator.close();
            recordIterator = null;
        }
    }

    @Override
    protected int getNext() throws IOException {
        try {
            return preExecutionAuthenticator.execute(() -> {
                int rows = 0;
                while (recordIterator.hasNext()) {
                    RowData record = recordIterator.next();
                    columnValue.setOffsetRow(record);
                    for (int i = 0; i < fields.length; i++) {
                        columnValue.setIdx(i, types[i]);
                        appendData(i, columnValue);
                    }
                    rows++;
                    if (rows >= batchSize) {
                        return rows;
                    }
                }
                return rows;
            });
        } catch (Exception e) {
            close();
            LOG.warn("Failed to get the next of setats. "
                            + "split: {}, requiredFieldNames: {}, setatsAllFieldNames: {}",
                    getSplit(), params.get("required_fields"), setatsAllFieldNames, e);
            throw new IOException(e);
        }
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    private void initTable() {
        table = SetatsUtils.deserialize(params.get("serialized_table"));
        setatsAllFieldNames = SetatsUtils.fieldNames(this.table.rowType());
        if (LOG.isDebugEnabled()) {
            LOG.debug("setatsAllFieldNames:{}", setatsAllFieldNames);
        }
    }

}
