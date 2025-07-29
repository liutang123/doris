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

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.SplitCreator;
import org.apache.doris.datasource.TableFormatType;

import com.google.common.collect.Maps;
import com.tencent.oceanus.table.source.Split;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class SetatsSplit extends FileSplit {
    private static final LocationPath DUMMY_PATH = new LocationPath("/dummyPath", Maps.newHashMap());
    private Split split;
    private TableFormatType tableFormatType;
    private Optional<byte[]> optDeletionVector;

    public SetatsSplit(Split split) {
        super(DUMMY_PATH, 0, 0, 0, 0, null, null);
        this.split = split;
        this.tableFormatType = TableFormatType.SETATS;
        this.optDeletionVector = Optional.empty();
        if (split instanceof DataSplit) {
            List<DataFileMeta> dataFileMetas = ((DataSplit) split).dataFiles();
            this.path = new LocationPath("/" + dataFileMetas.get(0).fileName());
            this.selfSplitWeight = dataFileMetas.stream().mapToLong(DataFileMeta::fileSize).sum();
        } else {
            this.selfSplitWeight = split.rowCount();
        }
    }

    public SetatsSplit(LocationPath file, long start, long length, long fileLength, long modificationTime,
            String[] hosts, List<String> partitionList) {
        super(file, start, length, fileLength, modificationTime, hosts, partitionList);
        this.tableFormatType = TableFormatType.SETATS;
        this.optDeletionVector = Optional.empty();
    }

    @Override
    public String getConsistentHashString() {
        if (this.path == DUMMY_PATH) {
            return UUID.randomUUID().toString();
        }
        return getPathString();
    }

    public Split getSplit() {
        return split;
    }

    public void setSplit(Split split) {
        this.split = split;
    }

    public TableFormatType getTableFormatType() {
        return tableFormatType;
    }

    public void setTableFormatType(TableFormatType tableFormatType) {
        this.tableFormatType = tableFormatType;
    }

    public void setDeletionVector(byte[] deletionVector) {
        this.optDeletionVector = Optional.of(deletionVector);
    }

    public Optional<byte[]> getOptDeletionVector() {
        return optDeletionVector;
    }

    public static class SetatsSplitCreator implements SplitCreator {

        static final SetatsSplitCreator DEFAULT = new SetatsSplitCreator();

        @Override
        public org.apache.doris.spi.Split create(LocationPath path,
                long start,
                long length,
                long fileLength,
                long fileSplitSize,
                long modificationTime,
                String[] hosts,
                List<String> partitionValues) {
            SetatsSplit split = new SetatsSplit(path, start, length, fileLength, modificationTime, hosts,
                    partitionValues);
            split.setTargetSplitSize(fileSplitSize);
            return split;
        }
    }
}
