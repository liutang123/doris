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

import com.google.common.collect.ImmutableList;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.util.TimeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


/*
 * SHOW PROC /dbs/dbId/tableId/partition_replicas/partitionId
 * show replicas' detail info within a partition
 */
public class PartitionReplicasProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId")
            .add("SchemaHash").add("Version")
            .add("LstSuccessVersion")
            .add("LstFailedVersion").add("LstFailedTime")
            .add("LstConsistencyCheckTime").add("CheckVersion")
            .add("DataSize").add("RowCount").add("State").add("IsBad").add("VersionCount").add("PathHash")
            .build();


    private DatabaseIf db;
    private OlapTable olapTable;
    private Partition partition;

    public PartitionReplicasProcNode(DatabaseIf db, OlapTable olapTable, Partition partition) {
        this.db = db;
        this.olapTable = olapTable;
        this.partition = partition;
    }

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                long tabletId = tablet.getId();
                for (Replica replica : tablet.getReplicas()) {
                    List<Object> replicaInfo = new ArrayList<>();
                    replicaInfo.add(tabletId);
                    replicaInfo.add(replica.getId());
                    replicaInfo.add(replica.getBackendId());
                    replicaInfo.add(replica.getSchemaHash());
                    replicaInfo.add(replica.getVersion());
                    replicaInfo.add(replica.getLastSuccessVersion());
                    replicaInfo.add(replica.getLastFailedVersion());
                    replicaInfo.add(TimeUtils.longToTimeString(replica.getLastFailedTimestamp()));
                    replicaInfo.add(TimeUtils.longToTimeString(tablet.getLastCheckTime()));
                    replicaInfo.add(tablet.getCheckedVersion());
                    replicaInfo.add(replica.getDataSize());
                    replicaInfo.add(replica.getRowCount());
                    replicaInfo.add(replica.getState());
                    replicaInfo.add(replica.isBad());
                    replicaInfo.add(replica.getVersionCount());
                    replicaInfo.add(replica.getPathHash());

                    result.addRow(replicaInfo.stream().map(Objects::toString).collect(Collectors.toList()));
                }
            }
        }
        return result;
    }
}
