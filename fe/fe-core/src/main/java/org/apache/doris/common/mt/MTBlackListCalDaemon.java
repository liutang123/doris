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

import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * @createTime: 2024/1/2 10:18
 * @Description: 计算BE黑名单，根据任务失败率及配置条件将BE节点加入/移除黑名单。
 * 1.be + taskType维度计算失败率，失败率过高且失败次数较多，将其放入对应taskType的黑名单中，已在黑名单中的BE不在被调度执行对应type的任务。
 * 2.已在黑名单中的BE，在黑名单中超过指定时间，则会将其从黑名单中移除。
 */
public class MTBlackListCalDaemon extends Daemon {
    private static final Logger LOG = LogManager.getLogger(MTBlackListCalDaemon.class);
    private static Map<List<String>, TaskMetricInfo> taskMetricInfoMap = new HashMap<>();
    private static Map<String, PriorityBlockingQueue<TaskMetricInfo>> blackListMap = new ConcurrentHashMap<>();

    public MTBlackListCalDaemon(String name, long intervalMs) {
        super(name, intervalMs);
    }

    @Override
    public void run() {
        if (metaContext != null) {
            metaContext.setThreadLocalInfo();
        }

        while (!isStop.get()) {
            try {
                runOneCycle();
            } catch (Throwable e) {
                LOG.error("daemon thread got exception. name: {}", getName(), e);
            }

            try {
                if (Config.mt_cal_be_blacklist_interval_seconds * 1000L != intervalMs) {
                    intervalMs = Config.mt_cal_be_blacklist_interval_seconds * 1000L;
                }
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException: ", e);
            }
        }

        if (metaContext != null) {
            MetaContext.remove();
        }
        LOG.error("daemon thread exits. name=" + this.getName());
    }

    @Override
    public void runOneCycle() {
        if (Config.mt_enable_backend_blacklist_cal) {
            calBlackList();
        }
    }

    public static Map<String, PriorityBlockingQueue<TaskMetricInfo>> getBlackListMap() {
        return blackListMap;
    }

    private void calBlackList() {
        long startTime = System.currentTimeMillis();
        Map<List<String>, LongCounterMetric> mulLabTaskTotalMap = MetricRepo.MT_COUNTER_SEND_TASKS.getMetricMap();
        Map<List<String>, LongCounterMetric> mulLabTaskFailedMap = MetricRepo.MT_COUNTER_TASK_FAILED_TOTAL.getMetricMap();
        Map<List<String>, LongCounterMetric> mulLabTaskReportMap = MetricRepo.MT_COUNTER_REPORT_TASKS.getMetricMap();
        Map<List<String>, LongCounterMetric> mulLabTaskReportFailedMap = MetricRepo.MT_COUNTER_REPORT_TASK_FAILED.getMetricMap();
        int aliveBeNum = Env.getCurrentEnv().getCurrentSystemInfo().getAllBackendIds(true).size();
        int limitNum = Math.min((int) (aliveBeNum * Config.mt_backend_blacklist_num_limit_ratio), Config.mt_backend_blacklist_num_limit_value);

        // 1.根据监控指标计算每台be，type维度的task失败率
        mulLabTaskTotalMap.entrySet().forEach(entry -> {
            List<String> labels = entry.getKey();
            calErrorRatio(mulLabTaskFailedMap, entry, labels);
        });

        // 单独计算Clone类型任务，作为SRC时的失败率
        mulLabTaskReportMap.entrySet().forEach(entry -> {
            List<String> labels = entry.getKey();
            String type = labels.get(1);
            if (!type.equals(FeConstants.clone_src)) {
                return;
            }
            calErrorRatio(mulLabTaskReportFailedMap, entry, labels);
        });

        // 2.根据失败率、失败次数等指标将be拉黑/移除
        taskMetricInfoMap.forEach((labels, metricInfo) -> {
            String host = labels.get(0);
            String type = labels.get(1);
            Double errRatio = metricInfo.errRatio;
            Long deltaFailNum = metricInfo.deltaFailNum;
            Boolean inBlackList = metricInfo.inBlackList;

            if (!inBlackList && errRatio > Config.mt_agent_task_err_ratio
                    && deltaFailNum > Config.mt_agent_task_err_num_min) {
                /* 2.1 拉黑有问题的be
                   - be失败率、失败次数超过限制，且type维度黑名单size不超过指定值，则直接拉黑
                   - be失败率、失败次数超过限制，但type维度黑名单size超过指定值，比较该be失败率是否高于黑名单中的be失败率
                 */
                PriorityBlockingQueue<TaskMetricInfo> blackList = blackListMap.computeIfAbsent(type,
                        key -> new PriorityBlockingQueue<>(limitNum, Comparator.comparingDouble(info -> info.errRatio)));
                if (!blackListIsContainHost(blackList, host)) {
                    int curSize = blackList.size();
                    if (curSize < limitNum) {
                        MetricRepo.MT_COUNTER_BLACKLIST_BE.getOrAdd(type).increase(1L);
                        metricInfo.toBlackListTime = System.currentTimeMillis();
                        metricInfo.inBlackList = true;
                        blackList.add(metricInfo);
                        LOG.info("{} task type's blacklist size {} not reach limit {}, add bad backend to blacklist. bad backend info:{}",
                                type, curSize, limitNum, metricInfo.toString());
                    } else {
                        TaskMetricInfo topElement = blackList.peek();
                        if (topElement.errRatio < errRatio) {
                            topElement.inBlackList = false;
                            blackList.poll();

                            metricInfo.toBlackListTime = System.currentTimeMillis();
                            metricInfo.inBlackList = true;
                            blackList.add(metricInfo);
                            LOG.info("{} task type's blacklist size {} reach limit {}, remove a relatively good backend, then add this backend to blacklist." +
                                    "remove backend info:{}, this backend info:{}",
                                    type, curSize, limitNum, topElement, metricInfo);
                        }
                    }
                }
            } else if (metricInfo.inBlackList) {
                /* 2.2 移除be
                    - 当be在黑名单中的时间超过一定阈值时，将其从黑名单中移除
                 */
                Long inBlackListTime = (System.currentTimeMillis() - metricInfo.toBlackListTime) / 1000;
                PriorityBlockingQueue<TaskMetricInfo> blackList = blackListMap.get(type);
                if (blackList != null && blackListIsContainHost(blackList, host)
                        && inBlackListTime >= Config.mt_in_blacklist_time_seconds ) {
                    blackList.remove(metricInfo);
                    blackListMap.put(type, blackList);
                    metricInfo.inBlackList = false;

                    MetricRepo.MT_COUNTER_BLACKLIST_BE.getOrAdd(type).increase(-1L);
                    LOG.info("remove backend, be:{} in task type's:{} blacklist time is {} seconds, exceed conf time {}.",
                            host, type, inBlackListTime, blackListMap.toString(), Config.mt_in_blacklist_time_seconds);
                }
            }
        });
        long endTime = System.currentTimeMillis();
        LOG.info("calculate backend blacklist elapsed: {}ms，blackList: {}", endTime - startTime, blackListMap.toString());
    }

    private void calErrorRatio(Map<List<String>, LongCounterMetric> taskMap, Map.Entry<List<String>,
            LongCounterMetric> entry, List<String> labels) {
        Long totalTaskNum = entry.getValue().getValue();
        LongCounterMetric metric = taskMap.get(labels);
        if (metric == null) {
            return;
        }
        Long failTaskNum = metric.getValue();
        TaskMetricInfo taskMetric = taskMetricInfoMap.computeIfAbsent(labels, key -> new TaskMetricInfo());
        if (taskMetric.inBlackList) {
            // 已经被拉黑，无需计算，因host, type已确定，拉黑期间该BE不会产生该type类型的增量任务
            return;
        }
        Long lastTotalTaskNum = taskMetric.totalNum;
        Long lastFailTaskNum = taskMetric.failNum;

        Long deltaTotalNum = totalTaskNum - lastTotalTaskNum;
        Long deltaFailNum = failTaskNum - lastFailTaskNum;
        double errRatio = 0.0;
        if (deltaTotalNum > 0) {
            errRatio = (double) deltaFailNum / deltaTotalNum;
        }
        taskMetric.totalNum = totalTaskNum;
        taskMetric.failNum = failTaskNum;
        taskMetric.deltaTotalNum = deltaTotalNum;
        taskMetric.deltaFailNum = deltaFailNum;
        taskMetric.errRatio = errRatio;
        taskMetric.host = labels.get(0);
    }

    private static boolean blackListIsContainHost(PriorityBlockingQueue<TaskMetricInfo> blackList, String host) {
        if (CollectionUtils.isEmpty(blackList)) {
            return false;
        }
        for (TaskMetricInfo info : blackList) {
            if (info.host.equals(host)) {
                return true;
            }
        }
        return false;
    }

    // 判断host是否存在黑名单中
    public static Boolean beInBlackList(String host, String type) {
        if (Config.mt_enable_backend_blacklist_cal) {
            PriorityBlockingQueue<TaskMetricInfo> blackList = blackListMap.get(type);
            if (blackList == null) {
                return false;
            }
            return blackListIsContainHost(blackList, host);
        }
        return false;
    }

    public static class TaskMetricInfo {
        public Long totalNum = 0L;
        public Long failNum = 0L;
        public Long deltaTotalNum = 0L;
        public Long deltaFailNum = 0L;
        public Double errRatio = 0.0;
        public Long toBlackListTime = 0L;
        public Boolean inBlackList = false;
        public String host;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskMetricInfo that = (TaskMetricInfo) o;
            return Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host);
        }

        @Override
        public String toString() {
            return "TaskMetricInfo{" +
                    "totalNum=" + totalNum +
                    ", failNum=" + failNum +
                    ", deltaTotalNum=" + deltaTotalNum +
                    ", deltaFailNum=" + deltaFailNum +
                    ", errRatio=" + errRatio +
                    ", toBlackListTime=" + toBlackListTime +
                    ", inBlackList=" + inBlackList +
                    ", host='" + host + '\'' +
                    '}';
        }
    }
}
