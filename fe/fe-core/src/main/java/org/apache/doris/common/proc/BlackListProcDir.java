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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.mt.MTBlackListCalDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * @createTime: 2024/1/25 15:24
 * @Description:
 */
public class BlackListProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(BlackListProcDir.class);

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TaskType")
            .build();

    Map<String, PriorityBlockingQueue<MTBlackListCalDaemon.TaskMetricInfo>> blackListMap;

    public BlackListProcDir(Map<String, PriorityBlockingQueue<MTBlackListCalDaemon.TaskMetricInfo>> blackListMap) {
        Preconditions.checkNotNull(blackListMap);
        this.blackListMap = blackListMap;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (Map.Entry<String, PriorityBlockingQueue<MTBlackListCalDaemon.TaskMetricInfo>> entry : blackListMap.entrySet()) {
            String taskType = entry.getKey();
            result.addRow(Lists.newArrayList(taskType));
        }

        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String taskType) throws AnalysisException {
        if (Strings.isNullOrEmpty(taskType)) {
            throw new AnalysisException("taskType is null");
        }
        PriorityBlockingQueue<MTBlackListCalDaemon.TaskMetricInfo> metricInfos = blackListMap.get(taskType);
        if (metricInfos == null) {
            metricInfos = new PriorityBlockingQueue<>();
        }
        return new BabBackendNode(metricInfos);
    }

    static class BabBackendNode implements ProcNodeInterface {

        public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
                .add("Host").add("TotalNum").add("FailNum")
                .add("DeltaTotalNum").add("DeltaFailNum").add("ErrRatio").add("ToBlackListTime")
                .build();

        private PriorityBlockingQueue<MTBlackListCalDaemon.TaskMetricInfo> metricInfos;
        public BabBackendNode(PriorityBlockingQueue<MTBlackListCalDaemon.TaskMetricInfo> metricInfos) {
            this.metricInfos = metricInfos;
        }

        @Override
        public ProcResult fetchResult() throws AnalysisException {
            BaseProcResult result = new BaseProcResult();
            result.setNames(TITLE_NAMES);
            if (CollectionUtils.isEmpty(metricInfos)) {
                return result;
            }
            for (MTBlackListCalDaemon.TaskMetricInfo metricInfo : metricInfos) {
                List<String> row = new ArrayList<>();
                row.add(metricInfo.host);
                row.add(String.valueOf(metricInfo.totalNum));
                row.add(String.valueOf(metricInfo.failNum));
                row.add(String.valueOf(metricInfo.deltaTotalNum));
                row.add(String.valueOf(metricInfo.deltaFailNum));
                row.add(String.valueOf(metricInfo.errRatio));
                row.add(dateFormat.format(new Date(metricInfo.toBlackListTime)));
                result.addRow(row);

            }
            return result;
        }
    }
}
