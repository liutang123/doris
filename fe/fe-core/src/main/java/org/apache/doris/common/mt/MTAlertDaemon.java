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

package org.apache.doris.common.mt;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MTAlertDaemon extends Daemon {

    private static final Logger LOG = LoggerFactory.getLogger(MTAlertDaemon.class);

    private static final Queue<String> MQ = new ConcurrentLinkedQueue<>();
    private static final int ALERT_MESSAGE_NUM = 100;
    private static final int ALERT_MESSAGE_LEN = 3000;
    private static final long MAX_INTERVAL_MS = 30 * 60 * 1000L;
    private static final long MIN_INTERVAL_MS = 3 * 60 * 1000L;

    public static List<Replica> biggestReplicas = new ArrayList<>();

    public static void warn(String message) {
        // non-strict
        if (MQ.size() < ALERT_MESSAGE_NUM) {
            MQ.offer(message);
        }
    }

    public static void error(String message, Throwable e) {
        StringWriter writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        warn(message + "\n" + writer);
    }

    public MTAlertDaemon() {
        super("mt-alert-daemon", MIN_INTERVAL_MS);
    }

    @Override
    protected void runOneCycle() {
        try {
            biggestReplicas = getBigReplicas(100);
        } catch (Exception e) {
            LOG.error("[MT] get biggest replicas error", e);
            error("get biggest replicas error", e);
        }

        if (MQ.isEmpty()) {
            this.setInterval(Math.max(MIN_INTERVAL_MS, this.getInterval() / 2));
        } else {
            StringBuilder builder = new StringBuilder(ALERT_MESSAGE_LEN * 2);
            while (!MQ.isEmpty() && builder.length() < ALERT_MESSAGE_LEN) {
                builder.append(MQ.poll()).append('\n');
            }
            MTUtil.getXmPusher().push(builder.toString(), Config.mt_alert_receivers);

            MQ.clear();
            this.setInterval(Math.min(MAX_INTERVAL_MS, this.getInterval() * 2));
        }
    }

    private List<Replica> getBigReplicas(int count) {
        PriorityQueue<Replica> heap = new PriorityQueue<>(count, Comparator.comparingLong(Replica::getDataSize));

        List<Database> allDB = new ArrayList<>(Env.getServingEnv().getInternalCatalog().getDbs());
        for (Database db : allDB) {
            for (Table table : db.getTables()) {
                if (table.getType() != Table.TableType.OLAP) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    Collection<Partition> allPartitions = olapTable.getAllPartitions();
                    for (Partition partition : allPartitions) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                            for (Tablet tablet : index.getTablets()) {
                                for (Replica replica : tablet.getReplicas()) {
                                    Replica r = heap.peek();
                                    if (heap.size() < count || r == null || r.getDataSize() < replica.getDataSize()) {
                                        heap.add(replica);
                                        if (heap.size() > count) {
                                            heap.poll();
                                        }
                                    }
                                }
                            }
                        } // end for indices
                    } // end for partitions
                } finally {
                    olapTable.readUnlock();
                }
            } // end for tables
        } // end for dbs
        return new ArrayList<>(heap);
    }

}
