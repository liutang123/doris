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

package org.apache.doris.plugin.audit;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.stream.Collectors;


/*
 * This plugin will load stream load audit log to specified doris table at specified interval
 */
@Getter
public class StreamLoadAuditLoader extends AuditLoader {
    private static final Logger LOG = LogManager.getLogger(StreamLoadAuditLoader.class);

    public static final String AUDIT_LOG_TABLE = "stream_load_audit_log";

    private final PluginInfo pluginInfo;

    public StreamLoadAuditLoader() {
        super(AUDIT_LOG_TABLE, InternalSchema.STREAM_LOAD_AUDIT_SCHEMA.stream().map(ColumnDef::getName)
                .collect(Collectors.joining(",")));
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "StreamLoadAuditLoader", PluginType.AUDIT,
                "builtin audit loader, to load stream load audit log to internal table",
            DigitalVersion.fromString("2.1.0"), DigitalVersion.fromString("1.8.31"),
            StreamLoadAuditLoader.class.getName(), null, null);
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.STREAM_LOAD_FINISH;
    }

    public void fillLogBuffer(AuditEvent event, StringBuilder logBuffer) {
        StreamLoadAuditEvent streamLoadEvent = (StreamLoadAuditEvent) event;
        logBuffer.append(streamLoadEvent.loadId).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.txnId).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.label).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.comment).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.db).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.table).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.user).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.clientIp).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.twoPhaseCommit).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.groupCommit).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.status).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.message).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.totalRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.loadedRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.filteredRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.unselectedRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.cpuCostMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.peakUsedMemoryBytes).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.loadBytes).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.startTime).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.loadTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.beginTxnTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.streamLoadPutTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.readDataTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.writeDataTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.receiveDataTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.commitAndPublishTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.errorUrl).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(streamLoadEvent.existingJobStatus).append(AUDIT_TABLE_LINE_DELIMITER);
    }
}

