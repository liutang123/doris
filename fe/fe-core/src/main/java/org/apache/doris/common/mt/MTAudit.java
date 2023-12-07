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

import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.InlineViewRef;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetOperationStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.common.util.QueryPlannerProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SingleNodePlanner;
import org.apache.doris.proto.Data;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.meituan.inf.xmdlog.XMDLogFormat;

import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;


public class MTAudit {
    private static final Logger logger = LoggerFactory.getLogger(MTAudit.class);

    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault());
    // the limits of MT LogCenter is 1M(1024 * 1024); we use 1024 * 1000 to reserve space for header infos;
    // The header is used to splice the split logs together
    private static final int KAFKA_LOG_MAX_LENGTH = 1024 * 1000;

    private static class ExecutorHandler {
        static final ThreadPoolExecutor executor;

        static {
            executor = ThreadPoolManager.newDaemonCacheThreadPool(Config.mt_audit_threads_num, "audit-pool", false);
            ThreadPoolManager.registerThreadPoolMetric("audit-pool", executor);
        }
    }

    public static void logAgentTaskExec(String beHost, String type, String taskType, String status, String message, Long num) {
        String time = LocalDateTime.now().format(DATETIME_FORMAT);
        String feHostName = null;
        try {
            feHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.warn("obtain fe host failed. err: {}", e);
        }
        String finalFeHostName = feHostName;

        ExecutorHandler.executor.submit(() -> {
            try {
                XMDLogFormat format = XMDLogFormat.build();
                format.putTag("domain", Config.mt_domain);
                format.putTag("fe", finalFeHostName);
                format.putTag("be", beHost);
                format.putTag("time", time);
                format.putTag("type", type);
                format.putTag("taskType", taskType);
                format.putTag("taskStatus", status);
                format.putTag("message", message);
                format.putTag("num", String.valueOf(num));
                MTLogger.DORIS_AUDIT_AGENT_TASK.logger.info(format.toString());
            } catch (Throwable ex) {
                String error = "[MT] log agent task error.";
                logger.error(error, ex);
                MTAlertDaemon.error(error, ex);
            }
        });
    }

    public static void logConnection(ConnectContext ctx, String item) {
        String id = String.valueOf(ctx.getConnectionId());
        String user = ctx.getQualifiedUser();
        String userIp = ctx.getMysqlChannel().getRemoteHostPortString();

        ExecutorHandler.executor.submit(() -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode root = mapper.createObjectNode();
                root.put("connectionId", id);
                root.put("user", user);
                root.put("userIp", userIp);

                XMDLogFormat format = XMDLogFormat.build();
                format.putTag("domain", Config.mt_domain);
                format.putTag("item", item);
                format.putTag("detail", mapper.writeValueAsString(root));
                MTLogger.DORIS_AUDIT_EVENT.logger.info(format.toString());
            } catch (Throwable ex) {
                String error = "[MT] log connection error " + id;
                logger.error(error, ex);
                MTAlertDaemon.error(error, ex);
            }
        });
    }

    public static void logQueryBeforeExec(ConnectContext ctx, StmtExecutor executor) {
        String queryId = DebugUtil.printId(ctx.queryId());

        ExecutorHandler.executor.submit(() -> {
            try {
                OriginStatement originStmt = executor.getOriginStmt();
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode root = mapper.createObjectNode();
                root.put("originSql", originStmt.originStmt);
                root.put("idx", originStmt.idx);
                root.put("queryId", queryId);

                XMDLogFormat format = XMDLogFormat.build();
                format.putTag("domain", Config.mt_domain);
                format.putTag("item", "query.begin");
                format.putTag("detail", mapper.writeValueAsString(root));
                MTLogger.DORIS_AUDIT_EVENT.logger.info(format.toString());
            } catch (Throwable ex) {
                String error = "[MT] log query error " + queryId;
                logger.error(error, ex);
                MTAlertDaemon.error(error, ex);
            }
        });
    }

    /**
     * profile infos after exec, contains ExecutionStatus, Summary, Execution Summary
     */
    public static void logProfileAfterExec(ConnectContext ctx, String originStmt) {
        StmtExecutor executor = ctx.getExecutor();
        if (executor == null) {
            logger.info("[MT]Not find StmtExecutor in logProfileAfterExec()");
            return;
        }

        // query id
        String queryId = DebugUtil.printId(ctx.queryId());
        StatementBase parsedStmt = executor.getParsedStmt();
        if (parsedStmt != null) {
            if (!(parsedStmt instanceof QueryStmt)) {
                logger.info("[MT] Only collect profile of SelectStmt or SetOperationStmt; " + queryId);
                return;
            }
        }

        Callable<?> doLog = () -> {
            ObjectMapper mapper = new ObjectMapper();

            ObjectNode nameNode = mapper.createObjectNode();
            ObjectNode rootNode = mapper.createObjectNode();

            String dorisVersion = Version.DORIS_BUILD_VERSION;
            String type = "After";

            rootNode.put("QueryID", queryId);
            rootNode.put("Version", dorisVersion);
            rootNode.put("Type", type);
            rootNode.put("Domain", Config.mt_domain);

            QueryState state = ctx.getState();
            String status = "1";
            String errorCode = "";
            String errorType = "";
            String errorMessage = "";
            if (state.getStateType() == QueryState.MysqlStateType.ERR) {
                status = "0";
                errorCode = state.getErrorCode() == null ? "" : state.getErrorCode().name();
                if (state.isAnalysisError()) {
                    errorType = "ANALYSIS_ERR";
                } else if (state.getErrType() == QueryState.ErrType.ANALYSIS_ERR) {
                    errorType = "USER_ERROR";
                } else {
                    errorType = "INTERNAL_ERROR";
                }
                errorMessage = state.getErrorMessage();
            }

            rootNode.put("Status", status);

            // summary profile
            ObjectNode summaryNode = mapper.createObjectNode();
            String startTime = TimeUtils.longToTimeString(ctx.getStartTime());
            long currentTimestamp = System.currentTimeMillis();
            String endTime = TimeUtils.longToTimeString(currentTimestamp);
            String totalTime = DebugUtil.getPrettyStringMs(currentTimestamp - ctx.getStartTime());
            String queryState = state.toString();
            String user = ctx.getQualifiedUser();
            String defaultDB = ctx.getDatabase();
            String queryType = parsedStmt == null ? null : parsedStmt.getClass().getSimpleName();
            String isCached = String.valueOf(executor.isCached());

            summaryNode.put("StartTime", startTime);
            summaryNode.put("EndTime", endTime);
            summaryNode.put("TotalTime", totalTime);
            summaryNode.put("QueryType", queryType);
            summaryNode.put("QueryState", queryState);
            summaryNode.put("ErrorType", errorType);
            summaryNode.put("ErrorCode", errorCode);
            summaryNode.put("ErrorMessage", errorMessage);
            summaryNode.put("User", user);
            summaryNode.put("DefaultDB", defaultDB);
            summaryNode.put("IsCached", isCached);
            summaryNode.put("SqlStatement", originStmt);

            ObjectNode executionSummaryNode = mapper.createObjectNode();
            QueryPlannerProfile plannerProfile = executor.getPlannerProfile();
            Map<String, String> runtimeProfileInfo = plannerProfile.initRuntimeProfileInfo();
            runtimeProfileInfo.forEach((k, v) -> executionSummaryNode.put(k, v));
            summaryNode.set("ExecutionSummary", executionSummaryNode);
            rootNode.set("Summary", summaryNode);
            nameNode.set("Query", rootNode);

            XMDLogFormat format = XMDLogFormat.build();
            List<String> logs = splitStringByLength(mapper.writeValueAsString(nameNode), KAFKA_LOG_MAX_LENGTH, queryId, type);
            for (String log : logs) {
                format.putJson(log);
                MTLogger.DORIS_PROFILE.logger.info(format.toString());
            }

            return null;
        };

        ExecutorHandler.executor.submit(() -> {
            try {
                doLog.call();
            } catch (Throwable ex) {
                String error = "[MT] logProfileAfterExec error " + queryId;
                logger.error(error, ex);
                MTAlertDaemon.error(error, ex);
            }
        });

    }

    /**
     * profile infos before exec, contains Fragments and Instances
     */
    public static void logProfileBeforeExec(Coordinator coord) {
        if (coord == null) {
            logger.info("[MT]Not find coordinator in logProfileBeforeExec()");
            return;
        }
        Planner planner = coord.getPlanner();
        if (planner == null) {
            logger.info("[MT]Not find planner in logProfileBeforeExec()");
            return;
        }
        String queryId = DebugUtil.printId(coord.getQueryId());

        Callable<?> doLog = () -> {
            ObjectMapper mapper = new ObjectMapper();

            ObjectNode nameNode = mapper.createObjectNode();
            ObjectNode rootNode = mapper.createObjectNode();
            String dorisVersion = Version.DORIS_BUILD_VERSION;
            String type = "Before";
            rootNode.put("QueryID", queryId);
            rootNode.put("Version", dorisVersion);
            rootNode.put("Type", type);
            rootNode.put("Domain", Config.mt_domain);

            ObjectNode executionProfileNode = mapper.createObjectNode();
            List<PlanFragment> fragments = planner.getFragments();
            // fragment count of this query
            int fragmentCnt = fragments.size();
            // instance count of all fragments in this query
            int allInstanceCnt = 0;
            // index of fragments in profile, from 0 to fragmentCnt-1
            int fragmentIdx = 0;
            for (PlanFragment planFragment : fragments) {
                ObjectNode fragmentNode = mapper.createObjectNode();
                fragmentNode.put("FragmentId", fragmentIdx);
                PlanFragmentId fragmentId = planFragment.getFragmentId();

                List<TUniqueId> originInstanceIds = coord.getInstanceIds(fragmentId);
                allInstanceCnt += originInstanceIds.size();
                fragmentNode.put("InstanceNum", originInstanceIds.size());

                int instanceIdx = 0;
                for (TUniqueId originInstanceId : originInstanceIds) {
                    ObjectNode instanceNode = mapper.createObjectNode();
                    String instanceId = DebugUtil.printId(originInstanceId);
                    instanceNode.put("InstanceId", instanceId);

                    Backend backend = coord.findBackendByInstance(fragmentId, instanceIdx);
                    instanceIdx += 1;
                    instanceNode.put("Host", backend.getHost());
                    instanceNode.put("BackendId", backend.getId());
                    fragmentNode.set("Instance " + instanceId, instanceNode);
                }
                executionProfileNode.set("Fragment " + fragmentIdx, fragmentNode);
                fragmentIdx++;
            }
            executionProfileNode.put("FragmentNum", fragmentCnt);
            executionProfileNode.put("AllInstanceNum", allInstanceCnt);

            rootNode.set("ExecutionProfile", executionProfileNode);
            nameNode.set("Query", rootNode);

            //The splice and nested json format will bring in extra escape characters.
            //One fragment's info need 6 escape chars and one instance's info need 12.
            int extraLength = fragmentCnt * 6 + allInstanceCnt * 12;


            XMDLogFormat format = XMDLogFormat.build();
            List<String> logs = splitStringByLength(mapper.writeValueAsString(nameNode), Math.max(KAFKA_LOG_MAX_LENGTH / 2, KAFKA_LOG_MAX_LENGTH - extraLength), queryId, type);
            for (String log : logs) {
                format.putJson(log);
                MTLogger.DORIS_PROFILE.logger.info(format.toString());
            }
            return null;
        };

        ExecutorHandler.executor.submit(() -> {
            try {
                doLog.call();
            } catch (Throwable ex) {
                String error = "[MT] logProfileBeforeExec error " + queryId;
                logger.error(error, ex);
                MTAlertDaemon.error(error, ex);
            }
        });
    }

    public static void logProfileWhenRpcError(Coordinator coord, TStatusCode code, String hostname) {
        if (coord == null) {
            logger.info("[MT]Not find coordinator in logProfileWhenRpcError()");
            return;
        }
        String queryId = DebugUtil.printId(coord.getQueryId());

        Callable<?> doLog = () -> {
            ObjectMapper mapper = new ObjectMapper();

            ObjectNode nameNode = mapper.createObjectNode();
            ObjectNode rootNode = mapper.createObjectNode();
            String dorisVersion = Version.DORIS_BUILD_VERSION;
            String type = "RpcError";

            rootNode.put("QueryID", queryId);
            rootNode.put("Version", dorisVersion);
            rootNode.put("Type", type);
            rootNode.put("Domain", Config.mt_domain);
            rootNode.put("RpcErrorCode", String.valueOf(code));
            rootNode.put("RpcErrorHost", hostname);
            rootNode.put("RpcErrorMsg", coord.getExecStatus().getErrorMsg());
            nameNode.set("Query", rootNode);

            XMDLogFormat format = XMDLogFormat.build();
            List<String> logs = splitStringByLength(mapper.writeValueAsString(nameNode), KAFKA_LOG_MAX_LENGTH, queryId, type);
            for (String log : logs) {
                format.putJson(log);
                MTLogger.DORIS_PROFILE.logger.info(format.toString());
            }

            return null;
        };

        ExecutorHandler.executor.submit(() -> {
            try {
                doLog.call();
            } catch (Throwable ex) {
                String error = "[MT] logProfileWhenRpcError error " + queryId;
                logger.error(error, ex);
                MTAlertDaemon.error(error, ex);
            }
        });
    }

    public static void logQueryPlan(Coordinator coord) {
        if (!Config.mt_audit_query_plan) return;

        if (coord == null) return;
        Planner planner = coord.getPlanner();
        // todo sjw: 完善nereidsPlanner审计逻辑
        if (planner == null || planner instanceof NereidsPlanner) return;
        SingleNodePlanner sPlanner = planner.getSingleNodePlanner();
        if (sPlanner == null) return;
        String queryId = DebugUtil.printId(coord.getQueryId());

        Callable<?> doLog = () -> {
            ObjectMapper mapper = new ObjectMapper();

            Set<Integer> scanNodeIds = new HashSet<>();
            for (ScanNode scanNode : sPlanner.getScanNodes()) {
                scanNodeIds.add(scanNode.getId().asInt());
            }

            XMDLogFormat format = XMDLogFormat.build();
            format.putTag("domain", Config.mt_domain);
            format.putTag("item", "query.plan.mv");
            for (Analyzer selectStmt : sPlanner.getSelectStmtToMVSelector().keySet()) {
                ObjectNode root = mapper.createObjectNode();
                root.put("queryId", queryId);
                sPlanner.getSelectStmtToMVSelector().get(selectStmt).writeExplainJson(root.putObject("MVSelector"));
                ArrayNode scanNodes = root.putArray("scanNodes");
                for (ScanNode scanNode : sPlanner.getSelectStmtToScanNodes().get(selectStmt)) {
                    int id = scanNode.getId().asInt();
                    if (scanNodeIds.contains(id)) {
                        scanNodes.add(id);
                    }
                }
                format.putTag("detail", mapper.writeValueAsString(root));
                MTLogger.DORIS_AUDIT_EVENT.logger.info(format.toString());
            }
            format.putTag("item", "query.plan.fragment");
            for (PlanFragment planFragment : planner.getFragments()) {
                ObjectNode root = mapper.createObjectNode();
                root.put("queryId", queryId);
                planFragment.writeExplainJson(root);
                ArrayNode instanceIds = root.putArray("instanceIds");
                for (TUniqueId id : coord.getInstanceIds(planFragment.getFragmentId())) {
                    instanceIds.add(DebugUtil.printId(id));
                }
                format.putTag("detail", mapper.writeValueAsString(root));
                MTLogger.DORIS_AUDIT_EVENT.logger.info(format.toString());
            }
            return null;
        };

        ExecutorHandler.executor.submit(() -> {
            try {
                doLog.call();
            } catch (Throwable ex) {
                String error = "[MT] log query plan error " + queryId;
                logger.error(error, ex);
                MTAlertDaemon.error(error, ex);
            }
        });
    }

    public static void logQueryAfterExec(ConnectContext ctx, String originStmt, long start) {
        /* audit all query
        if (parsedStmt != null) {
            String stmtClassSimpleName = parsedStmt.getClass().getSimpleName().toLowerCase();
            // in the daily query, to shorten the response of the page,
            // we focus on user queries, no query sql will be filtered. such as show sql and set sql
            if ((stmtClassSimpleName.startsWith("select") && !originStmt.toLowerCase().contains("from"))
                    || stmtClassSimpleName.startsWith("show")
                    || stmtClassSimpleName.startsWith("set")) {
                return;
            }
        }*/

        //String cluster = ctx.getClusterName();
        String qualifiedDbName = ctx.getDatabase(); // should be "cluster:db"
        String connectionId = String.valueOf(ctx.getConnectionId());
        String user = ctx.getQualifiedUser();
        String userIp = ctx.getMysqlChannel().getRemoteHostPortString();
        long end = System.currentTimeMillis();
        String queryId = DebugUtil.printId(ctx.queryId());
        StmtExecutor executor = ctx.getExecutor();
        long returnRows = ctx.getReturnRows();

        QueryState state = ctx.getState();
        String isQuery = state.isQuery() ? "1" : "0";
        String success, errorCode, errorType, errorMessage;
        if (state.getStateType() == QueryState.MysqlStateType.ERR) {
            success = "0";
            errorCode = state.getErrorCode() == null ? "" : state.getErrorCode().name();
            if (ctx.getState().isAnalysisError()) {
                // 审计中，我们只关心不是ANALYSIS_ERR的查询
                errorType = "ANALYSIS_ERR";
            } else if (ctx.getState().getErrType() == QueryState.ErrType.ANALYSIS_ERR) {
                // 所有的UserException都会被标记成ANALYSIS_ERR，然而，有些错误，如IO错误，获取副本错误并不能通过error_type判定
                errorType = "USER_ERROR";
            } else {
                errorType = "INTERNAL_ERROR";
            }
            errorMessage = state.getErrorMessage();
        } else {
            success = "1";
            errorCode = "";
            errorType = "";
            errorMessage = "";
        }
        // TODO yangzheng13
        // String cacheMode = ctx.getCacheMode().name();
        // String cacheKey = ctx.getCacheKey() == null ? "" : ctx.getCacheKey();

        Callable<?> doLog = () -> {
            int idx = qualifiedDbName.indexOf(":");
            String db = idx >= 0 ? qualifiedDbName.substring(idx + 1) : qualifiedDbName;

            StatementBase parsedStmt = executor == null ? null : executor.getParsedStmt();
            // We put origin query stmt at the end of audit log, for parsing the log more convenient.
            String sql;
            if (parsedStmt == null || parsedStmt.getOrigStmt() == null) {
                sql = originStmt;
            } else if (parsedStmt.needAuditEncryption()) {
                sql = parsedStmt.toSql();
            } else {
                sql = parsedStmt.getOrigStmt().originStmt;
            }
            String encodedSql = URLEncoder.encode(sql, "UTF-8").replaceAll("\\+", "%20");
            String table;
            if (parsedStmt == null) {
                table = "";
            } else if (parsedStmt instanceof InsertStmt) {
                table = ((InsertStmt) parsedStmt).getTbl();
            } else if (parsedStmt instanceof QueryStmt) {
                table = getTableNames((QueryStmt) parsedStmt);
            } else if (parsedStmt instanceof AlterTableStmt) {
                table = ((AlterTableStmt) parsedStmt).getTbl().getTbl();
            } else if (parsedStmt instanceof AlterViewStmt) {
                table = ((AlterViewStmt) parsedStmt).getTbl().getTbl();
            } else if (parsedStmt instanceof CreateMaterializedViewStmt) {
                table = ((CreateMaterializedViewStmt) parsedStmt).getBaseIndexName();
            } else {
                table = "";
            }

            Data.PQueryStatistics statistics = executor == null ? null : executor.getQueryStatisticsForAuditLog();
            Planner planner = executor == null ? null : executor.planner();
            Coordinator coordinator = executor == null ? null : executor.coordinator();
            int instanceNum = coordinator == null ? 0 : coordinator.getInstanceNum();
            List<ScanNode> scanNodes = planner == null ? Collections.emptyList() : planner.getScanNodes();
            int scanPartitionNum = 0, nonPartitionPruned = 0, nonPreAgg = 0;
            List<String> rollupNames = Lists.newArrayList();
            List<String> scanTableNames = Lists.newArrayList();
            String rollupName = "";
            String scanTableName = "";
            for (ScanNode scanNode : scanNodes) {
                if (scanNode instanceof OlapScanNode) {
                    OlapScanNode node = (OlapScanNode) scanNode;
                    scanPartitionNum += node.getSelectedPartitionIds().size();
                    // the number of scan nodes with partition_num > 1 and scanning all partitions in this query.
                    nonPartitionPruned += node.isPartitionPruned() ? 0 : 1;
                    nonPreAgg += node.isPreAggregation() ? 0 : 1;
                    rollupNames.add(node.getSelectedIndexName());
                    scanTableNames.add(node.getOlapTable().getName());
                }
            }
            if ((!scanTableNames.isEmpty()) && (!rollupNames.isEmpty())) {
                scanTableName = scanTableNames.stream().collect(Collectors.joining(","));
                rollupName = rollupNames.stream().collect(Collectors.joining("," ));
            }

            XMDLogFormat format = XMDLogFormat.build();
            format.putTag("domain", Config.mt_domain);
            //format.putTag("cluster", cluster);
            format.putTag("db", db);
            format.putTag("connection_id", connectionId);
            format.putTag("user", user);
            format.putTag("user_ip", userIp);
            format.putTag("duration", String.valueOf(end - start));
            format.putTag("start_time", String.valueOf(start));
            format.putTag("end_time", String.valueOf(end));
            format.putTag("start_datetime", DATETIME_FORMAT.format(Instant.ofEpochMilli(start)));
            format.putTag("end_datetime", DATETIME_FORMAT.format(Instant.ofEpochMilli(end)));

            format.putTag("query_id", queryId);
            format.putTag("query_type", parsedStmt == null ? "" : parsedStmt.getClass().getSimpleName());
            format.putTag("success", success);
            format.putTag("error_code", errorCode);
            format.putTag("error_type", errorType);
            format.putTag("error_message", errorMessage);
            format.putTag("cache_mode", ctx.getCacheMode().name());
            format.putTag("cache_key", ctx.getCacheKey() == null ? "" : ctx.getCacheKey());
            format.putTag("result_count", String.valueOf(returnRows));
            format.putTag("scan_bytes", String.valueOf(statistics == null ? 0 : statistics.getScanBytes()));
            format.putTag("scan_rows", String.valueOf(statistics == null ? 0 : statistics.getScanRows()));
            format.putTag("cpu_ms", String.valueOf(statistics == null ? 0 : statistics.getCpuMs()));
            format.putTag("table", table);
            format.putTag("scan_tables", scanTableName);
            format.putTag("selected_rollup", rollupName);
            format.putTag("scan_node_num", String.valueOf(scanNodes.size()));
            format.putTag("scan_partition_num", String.valueOf(scanPartitionNum));
            // typo
            format.putTag("non_parition_prune", String.valueOf(nonPartitionPruned));
            format.putTag("non_preagg_scan", String.valueOf(nonPreAgg));
            format.putTag("instances_num", String.valueOf(instanceNum));
            format.putTag("sql", encodedSql);

            // TODO remove deprecated fields
            // duplicated with dt
            format.putTag("data_time", DATE_FORMAT.format(Instant.now()));
            // duplicated with _mt_servername
            format.putTag("host_name", InetAddress.getLocalHost().getHostName());
            // can replaced by (scan_node_num > 0)
            format.putTag("is_query", isQuery);

            MTLogger.DORIS_AUDIT.logger.info(format.toString());
            return null;
        };

        ExecutorHandler.executor.submit(() -> {
            try {
                doLog.call();
            } catch (Throwable ex) {
                String error = "[MT] log query audit error " + queryId;
                logger.error(error, ex);
                MTAlertDaemon.error(error, ex);
            }
        });
    }

    private static String getTableNames(QueryStmt queryStmt) {
        Queue<QueryStmt> stmts = new LinkedList<>();
        stmts.add(queryStmt);
        Set<String> tables = new HashSet<>();
        while (!stmts.isEmpty()) {
            QueryStmt stmt = stmts.poll();
            if (stmt == null) continue;
            if (stmt instanceof SelectStmt) {
                for (TableRef ref : ((SelectStmt) stmt).getTableRefs()) {
                    if (ref instanceof InlineViewRef) {
                        stmts.add(((InlineViewRef) ref).getViewStmt());
                    } else {
                        tables.add(ref.getName().toString());
                    }
                }
            } else if (stmt instanceof SetOperationStmt) {
                for (SetOperationStmt.SetOperand operand : ((SetOperationStmt) stmt).getOperands()) {
                    stmts.add(operand.getQueryStmt());
                }
            }
        }
        return tables.stream().sorted().collect(Collectors.joining(","));
    }

    private static List<String> splitStringByLength(String content, int length, String queryId, String type) {
        List<String> result = Lists.newArrayList();

        ObjectMapper mapper = new ObjectMapper();

        List<String> splitedContent = splitStringByLength(content, length);
        int count = splitedContent.size();
        int index = 0;
        try {
            for (String piece : splitedContent) {
                ObjectNode nameNode = mapper.createObjectNode();
                nameNode.put("QueryId", queryId);
                nameNode.put("Domain", Config.mt_domain);
                nameNode.put("Type", type);
                nameNode.put("Count", count);
                nameNode.put("Index", index++);
                nameNode.put("Content", piece);
                result.add(mapper.writeValueAsString(nameNode));
            }
        } catch (Exception e) {
            String error = "[MT] Format error when split profile info in fe, query_id = " + queryId;
            logger.error(error, e);
        }

        return result;
    }

    private static List<String> splitStringByLength(String input, int length) {
        List<String> list = Lists.newArrayList();
        if (input == null || input.isEmpty()) {
            return list;
        }
        if (input.length() > length) {
            int index = 0;
            while (index < input.length()) {
                if (index + length > input.length()) {
                    length = input.length() - index;
                }
                String subStr = input.substring(index, index + length);
                list.add(subStr);
                index += length;
            }
        } else {
            list.add(input);
        }
        return list;
    }
}
