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

package org.apache.doris.datasource.setats.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.setats.source.SetatsSplit.SetatsSplitCreator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TSetatsFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.tencent.oceanus.predicate.Predicate;
import com.tencent.oceanus.shaded.org.apache.flink.util.InstantiationUtil;
import com.tencent.oceanus.table.Table;
import com.tencent.oceanus.table.source.DataSplit;
import com.tencent.oceanus.table.source.RawFile;
import com.tencent.oceanus.table.source.ReadBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SetatsScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(SetatsScanNode.class);
    private SetatsSource source;
    private Table setatsTable = null;
    private Predicate predicate = null;
    private String serializedTable;

    public SetatsScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv, SessionVariable sv) {
        super(id, desc, "SETATS_SCAN_NODE", StatisticalType.SETATS_SCAN_NODE, needCheckColumnPriv, sv);
        this.source = new SetatsSource(desc);
    }

    @Override
    protected void doInitialize() throws UserException {
        setatsTable = source.getSetatsTable();
        super.doInitialize();
        serializedTable = encodeObjectToString(setatsTable);
    }

    @Override
    protected void convertPredicate() {
        SetatsPredicateConverter converter = new SetatsPredicateConverter(setatsTable.rowType());
        predicate = converter.convert(conjuncts);
    }

    @Override
    protected Optional<String> getSerializedTable() {
        return Optional.of(serializedTable);
    }

    private static final Base64.Encoder BASE64_ENCODER =
            Base64.getUrlEncoder().withoutPadding();

    public static <T> String encodeObjectToString(T t) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(t);
            return new String(BASE64_ENCODER.encode(bytes), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof SetatsSplit) {
            setSetatsParams(rangeDesc, (SetatsSplit) split);
        }
    }

    public void setSetatsParams(TFileRangeDesc rangeDesc, SetatsSplit setatsSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(setatsSplit.getTableFormatType().value());
        TSetatsFileDesc fileDesc = new TSetatsFileDesc();
        com.tencent.oceanus.table.source.Split split = setatsSplit.getSplit();
        if (split != null) {
            // use jni reader
            fileDesc.setSetatsSplit(encodeObjectToString(split));
        } else {
            // we only support parquet format now.
            rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        }
        setatsSplit.getOptDeletionVector().ifPresent(fileDesc::setDeletionVector);
        fileDesc.setSetatsColumnNames(
                source.getDesc().getSlots().stream().map(slot -> slot.getColumn().getName()).collect(
                        Collectors.joining(",")));
        fileDesc.setSetatsPredicate(encodeObjectToString(predicate));
        tableFormatFileDesc.setSetatsParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    private void addJniSplit(boolean ignoreJni, List<Split> splits, com.tencent.oceanus.table.source.Split split) {
        if (!ignoreJni) {
            splits.add(new SetatsSplit(split));
        }
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        boolean forceJniScanner = sessionVariable.isForceJniScanner();
        if (!forceJniScanner) {
            if (source.getSetatsTable().ttlSeconds() > 0) {
                // if data ttl is set, we force to use jni scanner to do the ttl filter.
                forceJniScanner = true;
            }
        }
        SessionVariable.IgnoreSplitType ignoreSplitType = SessionVariable.IgnoreSplitType
                .valueOf(sessionVariable.getIgnoreSplitType());
        List<Split> splits = new ArrayList<>();
        int[] projected = desc.getSlots().stream().mapToInt(
                        slot -> (setatsTable.rowType().getFieldNames().indexOf(slot.getColumn().getName())))
                .toArray();
        int parallelNum = sessionVariable.getParallelExecInstanceNum();
        ReadBuilder readBuilder = setatsTable.newReadBuilder()
                .withFilter(predicate)
                .withProjection(projected)
                .withParallelism(parallelNum * numBackends);
        if (predicate == null && limit > 0 && limit < Integer.MAX_VALUE) {
            readBuilder.withLimit((int) limit);
        }

        LOG.info("get splits from setats table");
        List<com.tencent.oceanus.table.source.Split> setatsSplits = readBuilder.newScan().plan().splits();
        LOG.info("get splits count: {}", setatsSplits.size());
        boolean supportNative = supportNativeReader();
        boolean ignoreJni = ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_JNI;
        for (com.tencent.oceanus.table.source.Split split : setatsSplits) {
            LOG.info("get split: {}", split);
            if (forceJniScanner || !supportNative || !(split instanceof DataSplit)) {
                addJniSplit(ignoreJni, splits, split);
                continue;
            }
            DataSplit dataSplit = (DataSplit) split;
            if (split.deletionFiles().isPresent()) {
                LOG.info("Use jni scanner for split because it has dv: {}", split);
                addJniSplit(ignoreJni, splits, split);
                continue;
            }
            Optional<List<RawFile>> optRawFiles = dataSplit.convertToRawFiles();
            if (!optRawFiles.isPresent()) {
                addJniSplit(ignoreJni, splits, split);
                continue;
            }

            List<RawFile> rawFiles = optRawFiles.get();
            List<byte[]> dvs = split.deletionVector().orElse(null);
            for (int i = 0; i < rawFiles.size(); i++) {
                RawFile file = rawFiles.get(i);
                byte[] dv = dvs == null ? null : dvs.get(i);
                LocationPath locationPath = new LocationPath(file.path(), source.getCatalog().getProperties());
                try {
                    List<Split> dorisSplits = FileSplitter.splitFile(
                            locationPath,
                            getRealFileSplitSize(0),
                            null,
                            file.length(),
                            -1,
                            true,
                            null,
                            SetatsSplitCreator.DEFAULT);
                    for (Split dorisSplit : dorisSplits) {
                        LOG.info("add doris split: {}", dorisSplit);
                        if (dv != null) {
                            ((SetatsSplit) dorisSplit).setDeletionVector(dv);
                        }
                        splits.add(dorisSplit);
                    }
                } catch (IOException e) {
                    throw new UserException("Setats error to split file: " + e.getMessage(), e);
                }
            }
        }
        return splits;
    }

    private boolean supportNativeReader() {
        String fileFormat = source.getFileFormat().toLowerCase();
        return fileFormat.equals("parquet");
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        // same as paimon.
        return new ArrayList<>();
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        HashMap<String, String> map = new HashMap<>(source.getCatalog().getProperties());
        source.getCatalog().getCatalogProperty().getHadoopProperties().forEach((k, v) -> {
            if (!map.containsKey(k)) {
                map.put(k, v);
            }
        });
        return map;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder sb = new StringBuilder(super.getNodeExplainString(prefix, detailLevel));

        sb.append(prefix).append("predicatesFromSetats:");
        if (predicate != null) {
            sb.append(" NONE\n");
        } else {
            sb.append("\n");
            sb.append(prefix).append(prefix).append(predicate).append("\n");
        }
        return sb.toString();
    }
}
