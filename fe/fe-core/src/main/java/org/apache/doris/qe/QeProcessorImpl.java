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

package org.apache.doris.qe;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.mt.MTAlertDaemon;
import org.apache.doris.common.mt.MTAudit;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.resource.workloadgroup.QueueToken.TokenState;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class QeProcessorImpl implements QeProcessor {

    private static final Logger LOG = LogManager.getLogger(QeProcessorImpl.class);
    private Map<TUniqueId, QueryInfo> coordinatorMap;

    private Map<TUniqueId, Integer> queryToInstancesNum;
    private Map<String, AtomicInteger> userToInstancesCount;
    private Map<String, AtomicInteger> userToQueryCount;
    private List<Expression> trafficLimitRules;

    public static final QeProcessor INSTANCE;

    static {
        INSTANCE = new QeProcessorImpl();
    }

    private ExecutorService writeProfileExecutor;

    private QeProcessorImpl() {
        coordinatorMap = new ConcurrentHashMap<>();
        // write profile to ProfileManager when query is running.
        writeProfileExecutor = ThreadPoolManager.newDaemonProfileThreadPool(1, 100,
                "profile-write-pool", true);
        queryToInstancesNum = new ConcurrentHashMap<>();
        userToInstancesCount = new ConcurrentHashMap<>();
        trafficLimitRules = Collections.emptyList();
        userToQueryCount = Maps.newConcurrentMap();
    }

    public void setTrafficLimitRules(String trafficLimitRules) {
        ExpressionParser parser = new SpelExpressionParser(
                new SpelParserConfiguration(SpelCompilerMode.IMMEDIATE, this.getClass().getClassLoader()));
        this.trafficLimitRules = Arrays.stream(trafficLimitRules.split(";"))
                .map(String::trim).map(parser::parseExpression).collect(Collectors.toList());
    }

    @Override
    public Coordinator getCoordinator(TUniqueId queryId) {
        QueryInfo queryInfo = coordinatorMap.get(queryId);
        if (queryInfo != null) {
            return queryInfo.getCoord();
        }
        return null;
    }

    @Override
    public List<Coordinator> getAllCoordinators() {
        List<Coordinator> res = new ArrayList<>();

        for (QueryInfo co : coordinatorMap.values()) {
            res.add(co.coord);
        }
        return res;
    }

    @Override
    public void registerQuery(TUniqueId queryId, Coordinator coord) throws UserException {
        registerQuery(queryId, new QueryInfo(coord));
    }

    @Override
    public void registerQuery(TUniqueId queryId, QueryInfo info) throws UserException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("register query id = " + DebugUtil.printId(queryId) + ", job: " + info.getCoord().getJobId());
        }
        final QueryInfo result = coordinatorMap.putIfAbsent(queryId, info);
        if (result != null) {
            throw new UserException("queryId " + queryId + " already exists");
        }
    }

    @Override
    public void registerInstances(TUniqueId queryId, Integer instancesNum) throws UserException {
        QueryInfo queryInfo = coordinatorMap.get(queryId);
        if (queryInfo == null) {
            LOG.warn("[MT] register query, QueryInfo not found, queryId={}", DebugUtil.printId(queryId));
            return;
        }
        ConnectContext context = queryInfo.getConnectContext();
        if (context == null) {
            LOG.warn("[MT] register query, ConnectContext not found, queryId={}", DebugUtil.printId(queryId));
            return;
        }
        String user = context.getQualifiedUser();
        if (Strings.isNullOrEmpty(user)) {
            LOG.warn("[MT] register query,  user not found, queryId={}", DebugUtil.printId(queryId));
            return;
        }

        int queryNum = context.getState().isQuery() ? 1 : 0;
        AtomicInteger userQueryCounter = userToQueryCount.computeIfAbsent(user, __ -> new AtomicInteger(0));
        int userQueryNum = userQueryCounter.addAndGet(queryNum);
        AtomicInteger userInstanceCounter = userToInstancesCount.computeIfAbsent(user, __ -> new AtomicInteger(0));
        int userInstanceNum = userInstanceCounter.addAndGet(instancesNum);

        // instance limit
        long maxQueryInstances = queryInfo.getConnectContext().getEnv().getAuth().getMaxQueryInstances(user);
        if (maxQueryInstances <= 0) {
            maxQueryInstances = Config.default_max_query_instances;
        }
        if (maxQueryInstances > 0 && userInstanceNum > maxQueryInstances) {
            // Many query can reach here.
            userQueryCounter.addAndGet(-queryNum);
            userInstanceCounter.addAndGet(-instancesNum);
            throw new UserException("reach max_query_instances " + maxQueryInstances);
        }

        // expression limit
        QueryLimitContext ctx = new QueryLimitContext(queryInfo, userInstanceNum, userQueryNum);
        queryInfo.coord.setLimitContext(GsonUtils.GSON.toJson(ctx));
        for (Expression exp : trafficLimitRules) {
            try {
                Boolean limit = exp.getValue(ctx, Boolean.class);
                if (limit == null) {
                    String message = String.format("[MT] parse traffic limit rule error (%s) null result", exp.getExpressionString());
                    LOG.warn(message);
                    MTAlertDaemon.warn(message);
                } else if (limit) {
                    userQueryCounter.addAndGet(-queryNum);
                    userInstanceCounter.addAndGet(-instancesNum);
                    throw new UserException(String.format("reach traffic limit, rule (%s)", exp.getExpressionString()));
                }
            } catch (UserException e) {
                throw e;
            } catch (Exception e) {
                String message = String.format("[MT] parse traffic limit rule error (%s)", exp.getExpressionString());
                LOG.error(message, e);
                MTAlertDaemon.error(message, e);
            }
        }
        queryToInstancesNum.put(queryId, instancesNum);
        userToInstancesCount.computeIfAbsent(user, __ -> new AtomicInteger(0)).addAndGet(instancesNum);
        MetricRepo.USER_COUNTER_QUERY_INSTANCE_BEGIN.getOrAdd(user).increase(instancesNum.longValue());
        MTAudit.logQueryPlan(queryInfo.getCoord());
        MTAudit.logProfileBeforeExec(queryInfo.getCoord());
    }

    public Map<String, Integer> getInstancesNumPerUser() {
        return Maps.transformEntries(userToInstancesCount, (ignored, value) -> value != null ? value.get() : 0);
    }

    @Override
    public void unregisterQuery(TUniqueId queryId) {
        QueryInfo queryInfo = coordinatorMap.remove(queryId);
        Integer instanceNum = queryToInstancesNum.remove(queryId);
        if (queryInfo == null) {
            LOG.warn("[MT] unregister query, id not found, queryId={}", DebugUtil.printId(queryId));
            return;
        }
        ConnectContext context = queryInfo.getConnectContext();
        if (context == null) {
            LOG.warn("[MT] unregister query, ConnectContext not found, queryId={}", DebugUtil.printId(queryId));
            return;
        }
        String user = context.getQualifiedUser();
        if (Strings.isNullOrEmpty(user)) {
            LOG.warn("[MT] unregister query, user not found, queryId={}", DebugUtil.printId(queryId));
            return;
        }

        if (instanceNum == null) {
            // 表示被限流（并发及实例数统计已回滚）或未注册
            LOG.warn("[MT] unregister query, instance num not found, queryId={}", DebugUtil.printId(queryId));
        } else {
            // 回滚实例数
            AtomicInteger instanceCounter = userToInstancesCount.get(user);
            if (instanceCounter == null) {
                LOG.warn("[MT] unregister query, instance counter not found, queryId={}, instance num={}",
                        DebugUtil.printId(queryId), instanceNum);
            } else {
                instanceCounter.addAndGet(-instanceNum);
            }
            // 回滚并发数
            AtomicInteger queryCounter = userToQueryCount.get(user);
            if (queryCounter == null) {
                LOG.warn("[MT] unregister query, query counter not found, queryId={}",
                        DebugUtil.printId(queryId));
            } else {
                if (context.getState().isQuery()) {
                    int value = queryCounter.addAndGet(-1);
                    if (value < 0) {
                        LOG.warn("[MT] After unregister {}, queryCounter become negative: {}",
                                DebugUtil.printId(queryId), value);
                    }
                }
            }
        }

        // commit hive tranaction if needed
        Env.getCurrentHiveTransactionMgr().deregister(DebugUtil.printId(queryId));
    }

    @Override
    public Map<String, QueryStatisticsItem> getQueryStatistics() {
        final Map<String, QueryStatisticsItem> querySet = Maps.newHashMap();
        for (Map.Entry<TUniqueId, QueryInfo> entry : coordinatorMap.entrySet()) {
            final QueryInfo info = entry.getValue();
            final ConnectContext context = info.getConnectContext();
            if (info.sql == null || context == null) {
                continue;
            }
            final String queryIdStr = DebugUtil.printId(info.getConnectContext().queryId());
            final QueryStatisticsItem item = new QueryStatisticsItem.Builder().queryId(queryIdStr)
                    .queryStartTime(info.getStartExecTime()).sql(info.getSql()).user(context.getQualifiedUser())
                    .connId(String.valueOf(context.getConnectionId())).db(context.getDatabase())
                    .catalog(context.getDefaultCatalog())
                    .fragmentInstanceInfos(info.getCoord().getFragmentInstanceInfos())
                    .profile(info.getCoord().getExecutionProfile().getExecutionProfile())
                    .isReportSucc(context.getSessionVariable().enableProfile()).build();
            querySet.put(queryIdStr, item);
        }
        return querySet;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params, TNetworkAddress beAddr) {
        if (params.isSetProfile()) {
            LOG.info("ReportExecStatus(): fragment_instance_id={}, query id={}, backend num: {}, ip: {}",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id),
                    params.backend_num, beAddr);
            if (LOG.isDebugEnabled()) {
                LOG.debug("params: {}", params);
            }
        }
        final TReportExecStatusResult result = new TReportExecStatusResult();

        if (params.isSetReportWorkloadRuntimeStatus()) {
            Env.getCurrentEnv().getWorkloadRuntimeStatusMgr().updateBeQueryStats(params.report_workload_runtime_status);
            if (!params.isSetQueryId()) {
                result.setStatus(new TStatus(TStatusCode.OK));
                return result;
            }
        }

        final QueryInfo info = coordinatorMap.get(params.query_id);

        if (info == null) {
            // There is no QueryInfo for StreamLoad, so we return OK
            if (params.query_type == TQueryType.LOAD) {
                result.setStatus(new TStatus(TStatusCode.OK));
            } else {
                result.setStatus(new TStatus(TStatusCode.RUNTIME_ERROR));
            }
            LOG.warn("ReportExecStatus() runtime error, query {} with type {} does not exist",
                    DebugUtil.printId(params.query_id), params.query_type);
            return result;
        }
        try {
            info.getCoord().updateFragmentExecStatus(params);
            if (params.isSetProfile()) {
                writeProfileExecutor.submit(new WriteProfileTask(params, info));
            }
        } catch (Exception e) {
            LOG.warn("Exception during handle report, response: {}, query: {}, instance: {}", result.toString(),
                    DebugUtil.printId(params.query_id), DebugUtil.printId(params.fragment_instance_id));
            return result;
        }
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    @Override
    public String getCurrentQueryByQueryId(TUniqueId queryId) {
        QueryInfo info = coordinatorMap.get(queryId);
        if (info != null && info.sql != null) {
            return info.sql;
        }
        return "";
    }

    public Map<String, QueryInfo> getQueryInfoMap() {
        Map<String, QueryInfo> retQueryInfoMap = Maps.newHashMap();
        Set<TUniqueId> queryIdSet = coordinatorMap.keySet();
        for (TUniqueId qid : queryIdSet) {
            QueryInfo queryInfo = coordinatorMap.get(qid);
            if (queryInfo != null) {
                retQueryInfoMap.put(DebugUtil.printId(qid), queryInfo);
            }
        }
        return retQueryInfoMap;
    }

    public static final class QueryInfo {
        private final ConnectContext connectContext;
        private final Coordinator coord;
        private final String sql;

        // from Export, Pull load, Insert
        public QueryInfo(Coordinator coord) {
            this(null, null, coord);
        }

        // from query
        public QueryInfo(ConnectContext connectContext, String sql, Coordinator coord) {
            this.connectContext = connectContext;
            this.coord = coord;
            this.sql = sql;
        }

        public ConnectContext getConnectContext() {
            return connectContext;
        }

        public Coordinator getCoord() {
            return coord;
        }

        public String getSql() {
            return sql;
        }

        public long getStartExecTime() {
            if (coord.getQueueToken() != null) {
                return coord.getQueueToken().getQueueEndTime();
            }
            return -1;
        }

        public long getQueueStartTime() {
            if (coord.getQueueToken() != null) {
                return coord.getQueueToken().getQueueStartTime();
            }
            return -1;
        }

        public long getQueueEndTime() {
            if (coord.getQueueToken() != null) {
                return coord.getQueueToken().getQueueEndTime();
            }
            return -1;
        }

        public TokenState getQueueStatus() {
            if (coord.getQueueToken() != null) {
                return coord.getQueueToken().getTokenState();
            }
            return null;
        }
    }

    public static final class QueryLimitContext {
        private final transient QueryInfo info;
        @SerializedName("instanceNum")
        private final int instanceNum;
        @SerializedName("userInstanceNum")
        private final int userInstanceNum;
        @SerializedName("userQueryNum")
        private final int userQueryNum;
        @SerializedName("scanSize")
        private long scanSize;
        @SerializedName("maxScanSize")
        private long maxScanSize;
        @SerializedName("maxUnprunedScanSize")
        private long maxUnprunedScanSize;
        @SerializedName("scanRows")
        private long scanRows;
        @SerializedName("maxScanRows")
        private long maxScanRows;
        @SerializedName("maxScanPartitionNum")
        private int maxScanPartitionNum;
        @SerializedName("scanInstanceNum")
        private int scanInstanceNum;
        @SerializedName("maxScanInstanceNum")
        private int maxScanInstanceNum;
        @SerializedName("dbs")
        private Set<String> dbs;
        @SerializedName("tables")
        private Set<String> tables;
        @SerializedName("tableScanPartitionNum")
        private Map<String, Integer> tableScanPartitionNum;
        @SerializedName("priority")
        private Integer priority;

        public QueryLimitContext(QueryInfo info, int userInstanceNum, int userQueryNum) {
            this.info = info;
            this.instanceNum = info.getCoord().getInstanceNum();
            this.userQueryNum = userQueryNum;
            this.userInstanceNum = userInstanceNum;
            // user 直接用 info.connectContext.qualifiedUser，参考 https://km.sankuai.com/page/727356486#id-表达式灵活限流
            this.computeScanNode();
            this.priority = info.getConnectContext().getSessionVariable().getExecPriority();
        }

        private void computeScanNode() {
            scanSize = 0L;
            maxScanSize = 0L;
            maxUnprunedScanSize = 0L;
            scanRows = 0L;
            maxScanRows = 0L;
            maxScanPartitionNum = 0;
            scanInstanceNum = 0;
            maxScanInstanceNum = 0;
            tables = Sets.newHashSet();
            dbs = Sets.newHashSet();
            tableScanPartitionNum = Maps.newHashMap();
            for (ScanNode scanNode : info.getCoord().getPlanner().getScanNodes()) {
                long r = scanNode.getCardinality();
                long s = (long) (r * scanNode.getAvgRowSize());
                scanSize += s;
                maxScanSize = Math.max(maxScanSize, s);
                scanRows += r;
                maxScanRows = Math.max(maxScanRows, r);
                if (scanNode instanceof OlapScanNode) {
                    OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                    dbs.add(olapScanNode.getTupleDesc().getRef().getName().getDb());
                    String tableName = olapScanNode.getTupleDesc().getRef().getName().getFullTableName();
                    tables.add(tableName);
                    int scanPartitionNum = olapScanNode.getSelectedPartitionIds().size();
                    tableScanPartitionNum.compute(tableName, (t, n) -> scanPartitionNum + (n == null ? 0 : n));
                    maxUnprunedScanSize = Math.max(maxUnprunedScanSize, olapScanNode.isPartitionPruned() ? 0L : s);
                    maxScanPartitionNum = Math.max(maxScanPartitionNum, scanPartitionNum);
                    scanInstanceNum += olapScanNode.getNumInstances();
                    maxScanInstanceNum = Math.max(maxScanInstanceNum, olapScanNode.getNumInstances());
                }
            }
        }

        public int getPriority () {
            return priority;
        }

        public QueryInfo getInfo () {
            return info;
        }

        public int getInstanceNum () {
            return instanceNum;
        }

        public int getUserInstanceNum () {
            return userInstanceNum;
        }

        public int getUserQueryNum () {
            return userQueryNum;
        }

        public long getScanSize () {
            return scanSize;
        }

        public long getMaxScanSize () {
            return maxScanSize;
        }

        public long getMaxUnprunedScanSize () {
            return maxUnprunedScanSize;
        }

        public long getScanRows () {
            return scanRows;
        }

        public long getMaxScanRows () {
            return maxScanRows;
        }

        public int getMaxScanPartitionNum () {
            return maxScanPartitionNum;
        }

        public int getScanInstanceNum () {
            return scanInstanceNum;
        }

        public int getMaxScanInstanceNum () {
            return maxScanInstanceNum;
        }

        public Set<String> getDbs () {
            return dbs;
        }

        public Set<String> getTables () {
            return tables;
        }

        public Map<String, Integer> getTableScanPartitionNum () {
            return tableScanPartitionNum;
        }
    }

    private class WriteProfileTask implements Runnable {
        private TReportExecStatusParams params;

        private QueryInfo queryInfo;

        WriteProfileTask(TReportExecStatusParams params, QueryInfo queryInfo) {
            this.params = params;
            this.queryInfo = queryInfo;
        }

        @Override
        public void run() {
            QueryInfo info = coordinatorMap.get(params.query_id);
            if (info == null) {
                return;
            }

            ExecutionProfile executionProfile = info.getCoord().getExecutionProfile();
            executionProfile.update(-1, false);
        }
    }
}
