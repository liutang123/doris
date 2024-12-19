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

package org.apache.doris.nereids.trees.plans;

/**
 * Types for all Plan in Nereids.
 */
public enum PlanType {
    // special
    GROUP_PLAN,
    UNKNOWN,

    // logical plans
    // logical relations
    LOGICAL_SQL_CACHE,
    LOGICAL_BOUND_RELATION,
    LOGICAL_CTE_CONSUMER,
    LOGICAL_FILE_SCAN,
    LOGICAL_EMPTY_RELATION,
    LOGICAL_ES_SCAN,
    LOGICAL_JDBC_SCAN,
    LOGICAL_ODBC_SCAN,
    LOGICAL_OLAP_SCAN,
    LOGICAL_TEST_SCAN,
    LOGICAL_ONE_ROW_RELATION,
    LOGICAL_SCHEMA_SCAN,
    LOGICAL_TVF_RELATION,
    LOGICAL_UNBOUND_ONE_ROW_RELATION,
    LOGICAL_UNBOUND_RELATION,
    LOGICAL_UNBOUND_TVF_RELATION,

    // logical sinks
    LOGICAL_FILE_SINK,
    LOGICAL_OLAP_TABLE_SINK,
    LOGICAL_HIVE_TABLE_SINK,
    LOGICAL_ICEBERG_TABLE_SINK,
    LOGICAL_JDBC_TABLE_SINK,
    LOGICAL_RESULT_SINK,
    LOGICAL_UNBOUND_OLAP_TABLE_SINK,
    LOGICAL_UNBOUND_HIVE_TABLE_SINK,
    LOGICAL_UNBOUND_JDBC_TABLE_SINK,
    LOGICAL_UNBOUND_RESULT_SINK,

    // logical others
    LOGICAL_AGGREGATE,
    LOGICAL_APPLY,
    LOGICAL_ASSERT_NUM_ROWS,
    LOGICAL_CHECK_POLICY,
    LOGICAL_CTE,
    LOGICAL_CTE_ANCHOR,
    LOGICAL_CTE_PRODUCER,
    LOGICAL_EXCEPT,
    LOGICAL_FILTER,
    LOGICAL_GENERATE,
    LOGICAL_HAVING,
    LOGICAL_INLINE_TABLE,
    LOGICAL_INTERSECT,
    LOGICAL_JOIN,
    LOGICAL_LIMIT,
    LOGICAL_MULTI_JOIN,
    LOGICAL_PARTITION_TOP_N,
    LOGICAL_PROJECT,
    LOGICAL_QUALIFY,
    LOGICAL_REPEAT,
    LOGICAL_SELECT_HINT,
    LOGICAL_SUBQUERY_ALIAS,
    LOGICAL_VIEW,
    LOGICAL_SORT,
    LOGICAL_TOP_N,
    LOGICAL_UNION,
    LOGICAL_USING_JOIN,
    LOGICAL_WINDOW,

    // physical plans
    // physical relations
    PHYSICAL_SQL_CACHE,
    PHYSICAL_CTE_CONSUMER,
    PHYSICAL_EMPTY_RELATION,
    PHYSICAL_ES_SCAN,
    PHYSICAL_FILE_SCAN,
    PHYSICAL_HUDI_SCAN,
    PHYSICAL_JDBC_SCAN,
    PHYSICAL_ODBC_SCAN,
    PHYSICAL_ONE_ROW_RELATION,
    PHYSICAL_OLAP_SCAN,
    PHYSICAL_SCHEMA_SCAN,
    PHYSICAL_TVF_RELATION,

    // physical sinks
    PHYSICAL_FILE_SINK,
    PHYSICAL_OLAP_TABLE_SINK,
    PHYSICAL_HIVE_TABLE_SINK,
    PHYSICAL_ICEBERG_TABLE_SINK,
    PHYSICAL_JDBC_TABLE_SINK,
    PHYSICAL_RESULT_SINK,

    // physical others
    PHYSICAL_HASH_AGGREGATE,
    PHYSICAL_ASSERT_NUM_ROWS,
    PHYSICAL_CTE_PRODUCER,
    PHYSICAL_CTE_ANCHOR,
    PHYSICAL_DISTRIBUTE,
    PHYSICAL_EXCEPT,
    PHYSICAL_FILTER,
    PHYSICAL_GENERATE,
    PHYSICAL_INTERSECT,
    PHYSICAL_HASH_JOIN,
    PHYSICAL_NESTED_LOOP_JOIN,
    PHYSICAL_LIMIT,
    PHYSICAL_PARTITION_TOP_N,
    PHYSICAL_PROJECT,
    PHYSICAL_REPEAT,
    PHYSICAL_LOCAL_QUICK_SORT,
    PHYSICAL_QUICK_SORT,
    PHYSICAL_TOP_N,
    PHYSICAL_UNION,
    PHYSICAL_WINDOW,

    // commands
    ADMIN_CHECK_TABLETS_COMMAND,
    CREATE_POLICY_COMMAND,
    CREATE_TABLE_COMMAND,
    CREATE_SQL_BLOCK_RULE_COMMAND,
    DELETE_COMMAND,
    EXPLAIN_COMMAND,
    EXPORT_COMMAND,
    INSERT_INTO_TABLE_COMMAND,
    BATCH_INSERT_INTO_TABLE_COMMAND,
    INSERT_OVERWRITE_TABLE_COMMAND,
    LOAD_COMMAND,
    SELECT_INTO_OUTFILE_COMMAND,
    UPDATE_COMMAND,
    CREATE_MTMV_COMMAND,
    CREATE_JOB_COMMAND,
    PAUSE_JOB_COMMAND,
    CANCEL_JOB_COMMAND,
    DROP_JOB_COMMAND,
    RESUME_JOB_COMMAND,
    ALTER_MTMV_COMMAND,
    ADD_CONSTRAINT_COMMAND,
    ADMIN_COMPACT_TABLE_COMMAND,
    DROP_CONSTRAINT_COMMAND,
    SHOW_CONSTRAINTS_COMMAND,
    REFRESH_MTMV_COMMAND,
    DROP_MTMV_COMMAND,
    PAUSE_MTMV_COMMAND,
    RESUME_MTMV_COMMAND,
    SHOW_CREATE_MTMV_COMMAND,
    CANCEL_EXPORT_COMMAND,
    CANCEL_LOAD_COMMAND,
    CANCEL_WARM_UP_JOB_COMMAND,
    CANCEL_MTMV_TASK_COMMAND,
    CALL_COMMAND,
    CREATE_PROCEDURE_COMMAND,
    DROP_PROCEDURE_COMMAND,
    DROP_ROLE_COMMAND,
    DROP_REPOSITOORY_COMMAND,
    SHOW_PROCEDURE_COMMAND,
    SHOW_CREATE_PROCEDURE_COMMAND,
    CREATE_VIEW_COMMAND,
    CLEAN_ALL_PROFILE_COMMAND,
    CLEAN_TRASH_COMMAND,
    CREATE_ROLE_COMMAND,
    ALTER_ROLE_COMMAND,
    ALTER_VIEW_COMMAND,
    ALTER_STORAGE_VAULT,
    ALTER_WORKLOAD_GROUP_COMMAND,
    ALTER_WORKLOAD_POLICY_COMMAND,
    DROP_CATALOG_RECYCLE_BIN_COMMAND,
    DROP_ENCRYPTKEY_COMMAND,
    DROP_FILE_COMMAND,
    UNSET_VARIABLE_COMMAND,
    UNSET_DEFAULT_STORAGE_VAULT_COMMAND,
    UNSUPPORTED_COMMAND,
    CREATE_TABLE_LIKE_COMMAND,
    SET_OPTIONS_COMMAND,
    SET_TRANSACTION_COMMAND,
    SET_USER_PROPERTIES_COMMAND,
    SET_DEFAULT_STORAGE_VAULT_COMMAND,
    REFRESH_CATALOG_COMMAND,
    REFRESH_DATABASE_COMMAND,
    REFRESH_TABLE_COMMAND,
    PREPARED_COMMAND,
    EXECUTE_COMMAND,
    DROP_SQL_BLOCK_RULE_COMMAND,
    DROP_USER_COMMAND,
    DROP_WORKLOAD_GROUP_NAME,
    DROP_WORKLOAD_POLICY_COMMAND,
    ADMIN_SET_TABLE_STATUS_COMMAND,
    ALTER_CATALOG_COMMENT_COMMAND,
    ALTER_SQL_BLOCK_RULE_COMMAND,
    SHOW_BACKENDS_COMMAND,
    SHOW_BLOCK_RULE_COMMAND,
    SHOW_BROKER_COMMAND,
    SHOW_CHARSET_COMMAND,
    SHOW_COLLATION_COMMAND,
    SHOW_CONFIG_COMMAND,
    SHOW_CREATE_CATALOG_COMMAND,
    SHOW_CREATE_DATABASE_COMMAND,
    SHOW_CREATE_MATERIALIZED_VIEW_COMMAND,
    SHOW_CREATE_REPOSITORY_COMMAND,
    SHOW_CREATE_TABLE_COMMAND,
    SHOW_CREATE_VIEW_COMMAND,
    SHOW_DATABASE_ID_COMMAND,
    SHOW_DATA_SKEW_COMMAND,
    SHOW_DELETE_COMMAND,
    SHOW_DIAGNOSE_TABLET_COMMAND,
    SHOW_DYNAMIC_PARTITION_COMMAND,
    SHOW_ENCRYPT_KEYS_COMMAND,
    SHOW_EVENTS_COMMAND,
    SHOW_FRONTENDS_COMMAND,
    SHOW_GRANTS_COMMAND,
    SHOW_LAST_INSERT_COMMAND,
    SHOW_LOAD_PROFILE_COMMAND,
    SHOW_PARTITIONID_COMMAND,
    SHOW_PROCESSLIST_COMMAND,
    SHOW_PROC_COMMAND,
    SHOW_PLUGINS_COMMAND,
    SHOW_PRIVILEGES_COMMAND,
    SHOW_REPLICA_DISTRIBUTION_COMMAND,
    SHOW_REPLICA_STATUS_COMMAND,
    SHOW_REPOSITORIES_COMMAND,
    SHOW_ROLE_COMMAND,
    SHOW_SMALL_FILES_COMMAND,
    SHOW_STORAGE_ENGINES_COMMAND,
    SHOW_SYNC_JOB_COMMAND,
    SHOW_TABLE_ID_COMMAND,
    SHOW_TRASH_COMMAND,
    SHOW_TABLET_STORAGE_FORMAT_COMMAND,
    SHOW_TRIGGERS_COMMAND,
    SHOW_VARIABLES_COMMAND,
    SHOW_AUTHORS_COMMAND,
    SHOW_VIEW_COMMAND,
    SHOW_WARNING_ERRORS_COMMAND,
    SHOW_WHITE_LIST_COMMAND,
    SHOW_TABLETS_BELONG_COMMAND,
    SYNC_COMMAND,
    RECOVER_DATABASE_COMMAND,
    RECOVER_TABLE_COMMAND,
    RECOVER_PARTITION_COMMAND,
    REPLAY_COMMAND,
    ADMIN_REBALANCE_DISK_COMMAND,
    ADMIN_CANCEL_REBALANCE_DISK_COMMAND,
    CREATE_ENCRYPTKEY_COMMAND,
    CREATE_WORKLOAD_GROUP_COMMAND,
    CREATE_CATALOG_COMMAND,
    CREATE_FILE_COMMAND,
    CREATE_ROUTINE_LOAD_COMMAND,
    SHOW_TABLE_CREATION_COMMAND,
    SHOW_QUERY_PROFILE_COMMAND,
    SWITCH_COMMAND
}
