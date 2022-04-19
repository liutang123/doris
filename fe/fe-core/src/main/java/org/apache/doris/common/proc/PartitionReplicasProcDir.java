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

package org.apache.doris.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/*
 * SHOW PROC /dbs/dbId/tableId/partition_replicas
 * include partitions and temp_partitions
 */
public class PartitionReplicasProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionId").add("PartitionName").add("isTemp")
            .add("VisibleVersion").add("VisibleVersionTime")
            .add("State").add("PartitionKey").add("Range").add("DistributionKey")
            .add("Buckets").add("ReplicationNum").add("StorageMedium").add("CooldownTime")
            .add("LastConsistencyCheckTime")
            .add("DataSize")
            .add("IsInMemory")
            .build();

    private DatabaseIf db;
    private OlapTable olapTable;

    public PartitionReplicasProcDir(DatabaseIf db, OlapTable olapTable) {
        this.db = db;
        this.olapTable = olapTable;
    }

    private List<Object> getPartitionInfo(Long partitionId, boolean isTemp) {
        PartitionInfo tblPartitionInfo = olapTable.getPartitionInfo();
        Partition partition = olapTable.getPartition(partitionId);

        List<Object> partitionInfo = new ArrayList<>();
        String partitionName = partition.getName();
        partitionInfo.add(partitionId);
        partitionInfo.add(partitionName);
        partitionInfo.add(isTemp);
        partitionInfo.add(partition.getVisibleVersion());
        partitionInfo.add(TimeUtils.longToTimeString(partition.getVisibleVersionTime()));
        partitionInfo.add(partition.getState());

        if (tblPartitionInfo.getType() == PartitionType.RANGE || tblPartitionInfo.getType() == PartitionType.LIST) {
            String partitionColumns = tblPartitionInfo.getPartitionColumns().stream()
                    .map(Column::getName).collect(Collectors.joining(", "));
            partitionInfo.add(partitionColumns);
            partitionInfo.add(tblPartitionInfo.getItem(partitionId).getItems().toString());
        } else {
            partitionInfo.add("");
            partitionInfo.add("");
        }

        // distribution
        DistributionInfo distributionInfo = partition.getDistributionInfo();
        if (distributionInfo.getType() == DistributionInfoType.HASH) {
            String distributionColumns = ((HashDistributionInfo) distributionInfo).getDistributionColumns().stream()
                    .map(Column::getName).collect(Collectors.joining(", "));
            partitionInfo.add(distributionColumns);
        } else {
            partitionInfo.add("ALL KEY");
        }

        partitionInfo.add(distributionInfo.getBucketNum());

        short replicationNum = tblPartitionInfo.getReplicaAllocation(partitionId).getTotalReplicaNum();
        partitionInfo.add(String.valueOf(replicationNum));

        DataProperty dataProperty = tblPartitionInfo.getDataProperty(partitionId);
        partitionInfo.add(dataProperty.getStorageMedium().name());
        partitionInfo.add(TimeUtils.longToTimeString(dataProperty.getCooldownTimeMs()));

        partitionInfo.add(TimeUtils.longToTimeString(partition.getLastCheckTime()));

        long dataSize = partition.getDataSize(false);
        Pair<Double, String> sizePair = DebugUtil.getByteUint(dataSize);
        String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(sizePair.first) + " "
                + sizePair.second;
        partitionInfo.add(readableSize);
        partitionInfo.add(tblPartitionInfo.getIsInMemory(partitionId));

        return partitionInfo;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkState(olapTable.getType() == TableIf.TableType.OLAP);

        List<List<String>> partitionInfos;
        olapTable.readLock();
        try {
            PartitionInfo tblPartitionInfo = olapTable.getPartitionInfo();
            Stream<List<Object>> partitions;
            Stream<List<Object>> tempPartitions;

            // for range partitions, we return partitions in ascending range order by default.
            // this is to be consistent with the behaviour before 0.12
            if (tblPartitionInfo.getType() == PartitionType.RANGE || tblPartitionInfo.getType() == PartitionType.LIST) {
                partitions = tblPartitionInfo.getPartitionItemEntryList(false, true).stream()
                        .map(e -> getPartitionInfo(e.getKey(), false));
                tempPartitions = tblPartitionInfo.getPartitionItemEntryList(true, true).stream()
                        .map(e -> getPartitionInfo(e.getKey(), true));
            } else {
                partitions = olapTable.getPartitions().stream().map(p -> getPartitionInfo(p.getId(), false));
                tempPartitions = olapTable.getTempPartitions().stream().map(p -> getPartitionInfo(p.getId(), true));
            }
            partitionInfos = Stream.concat(partitions, tempPartitions)
                    .map(l -> l.stream().map(Objects::toString).collect(Collectors.toList()))
                    .collect(Collectors.toList());
        } finally {
            olapTable.readUnlock();
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.setRows(partitionInfos);
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionIdStr) throws AnalysisException {
        long partitionId = -1L;
        try {
            partitionId = Long.parseLong(partitionIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid partition id format: " + partitionIdStr);
        }

        olapTable.readLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new AnalysisException("Partition[" + partitionId + "] does not exist");
            }

            return new PartitionReplicasProcNode(db, olapTable, partition);
        } finally {
            olapTable.readUnlock();
        }
    }
}
