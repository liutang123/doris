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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.mt.MTAlertDaemon;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * SHOW PROC /tablet_inverted_index
 */
public class TabletInvertedIndexProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Index").build();

    public static final String BACKENDS = "backends";
    public static final String BIGGEST = "biggest";

    private final Env env;
    private final TabletInvertedIndex invertedIndex;
    private final SystemInfoService clusterInfoService;

    public TabletInvertedIndexProcDir(Env env) {
        this.env = env;
        this.invertedIndex = env.getTabletInvertedIndex();
        this.clusterInfoService = env.getCurrentSystemInfo();
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String path) throws AnalysisException {
        if (BACKENDS.equals(path)) {
            return new BackendsTabletsProcDir();
        } else if (BIGGEST.equals(path)) {
            return new BiggestReplicasProcNode();
        } else {
            throw new AnalysisException("index not found");
        }
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.setRows(Stream.of(BACKENDS, BIGGEST).map(Collections::singletonList).collect(Collectors.toList()));
        return result;
    }

    /*
     * SHOW PROC /tablet_inverted_index/backends
     */
    public class BackendsTabletsProcDir implements ProcDirInterface {

        public final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
                .add("BackendId").add("IP").add("HostName").add("Alive")
                .add("SystemDecommissioned").add("TabletNum")
                .build();

        @Override
        public boolean register(String name, ProcNodeInterface node) {
            return false;
        }

        @Override
        public ProcNodeInterface lookup(String path) throws AnalysisException {
            if (Strings.isNullOrEmpty(path)) {
                throw new AnalysisException("");
            }

            try {
                return new BackendTabletsProcNode(Long.parseLong(path));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid be id: " + path);
            }

        }

        @Override
        public ProcResult fetchResult() throws AnalysisException {
            BaseProcResult result = new BaseProcResult();
            result.setNames(TITLE_NAMES);

            List<Long> backendIds = clusterInfoService.getAllBackendIds(false);

            List<List<Comparable>> comparableBackendInfos = new ArrayList<>();
            for (long backendId : backendIds) {
                Backend backend = clusterInfoService.getBackend(backendId);
                if (backend == null) {
                    continue;
                }

                List<Comparable> backendInfo = Lists.newArrayList();
                backendInfo.add(String.valueOf(backendId));
                //backendInfo.add(backend.getOwnerClusterName());
                backendInfo.add(backend.getHost());
                backendInfo.add(NetUtils.getHostnameByIp(backend.getHost()));
                backendInfo.add(String.valueOf(backend.isAlive()));
                if (backend.isDecommissioned()) {
                    backendInfo.add("true");
                } else {
                    backendInfo.add("false");
                }
                backendInfo.add(Integer.toString(invertedIndex.getTabletNumByBackendId(backendId)));
                comparableBackendInfos.add(backendInfo);
            }

            // sort by cluster name, host name
            List<List<String>> backendInfos = comparableBackendInfos.stream()
                    .sorted(new ListComparator<>(1, 3))
                    .map(s -> s.stream().map(Object::toString).collect(Collectors.toList()))
                    .collect(Collectors.toList());

            result.setRows(backendInfos);
            return result;
        }
    }

    /*
     * SHOW PROC /tablet_inverted_index/backends/backendId
     */
    public class BackendTabletsProcNode implements ProcNodeInterface {

        public final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
                .add("TabletId").add("ReplicaId").add("BackendId").build();

        private final long backendId;

        public BackendTabletsProcNode(long backendId) {
            this.backendId = backendId;
        }

        @Override
        public ProcResult fetchResult() throws AnalysisException {
            BaseProcResult result = new BaseProcResult();
            result.setNames(TITLE_NAMES);

            List<List<String>> tabletInfos = invertedIndex.getReplicasByBackendId(backendId)
                    .entrySet().stream().map(kv -> {
                        Long tabletId = kv.getKey();
                        Replica replica = kv.getValue();
                        return Lists.<Comparable>newArrayList(tabletId, replica.getId(), backendId);
                    }).sorted(new ListComparator<>(0, 1))
                    .map(s -> s.stream().map(Object::toString).collect(Collectors.toList()))
                    .collect(Collectors.toList());
            result.setRows(tabletInfos);
            return result;
        }
    }

    /*
     * SHOW PROC /tablet_inverted_index/biggest
     */
    public class BiggestReplicasProcNode implements ProcNodeInterface {

        public final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
                .add("TabletId").add("ReplicaId").add("BackendId").build();

        public BiggestReplicasProcNode() {
        }

        @Override
        public ProcResult fetchResult() throws AnalysisException {
            BaseProcResult result = new BaseProcResult();
            result.setNames(TITLE_NAMES);

            TabletInvertedIndex index = Env.getCurrentInvertedIndex();
            List<List<String>> info = MTAlertDaemon.biggestReplicas.stream()
                    .map(r -> Lists.newArrayList(index.getTabletIdByReplica(r.getId()), r.getId(), r.getBackendId()))
                    .map(l -> l.stream().map(i -> i == null ? -1L : i).map(String::valueOf).collect(Collectors.toList()))
                    .collect(Collectors.toList());
            result.setRows(info);
            return result;
        }
    }
}
