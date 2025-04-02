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

import org.apache.doris.plugin.AuditEvent;

public class StreamLoadAuditEvent extends AuditEvent {

    public static final String Load_ID = "LoadId";
    public static final String TXN_ID = "TxnId";
    public static final String LABEL = "Label";
    public static final String COMMENT = "Comment";
    public static final String DB = "Db";
    public static final String TABLE = "Table";
    public static final String USER = "User";
    public static final String CLIENT_IP = "ClientIp";
    public static final String TWO_PHASE_COMMIT = "TwoPhaseCommit";
    public static final String GROUP_COMMIT = "GroupCommit";
    public static final String STATUS = "Status";
    public static final String MESSAGE = "Message";
    public static final String NUMBER_TOTAL_ROWS = "NumberTotalRows";
    public static final String NUMBER_LOADED_ROWS = "NumberLoadedRows";
    public static final String NUMBER_FILTERED_ROWS = "NumberFilteredRows";
    public static final String NUMBER_UNSELECTED_ROWS = "NumberUnselectedRows";
    public static final String CPU_COST_MS = "CpuCostMs";
    public static final String PEAK_USED_MEMORY_BYTES = "PeakUsedMemoryBytes";
    public static final String LOAD_BYTES = "LoadBytes";
    public static final String LOAD_TIME_MS = "LoadTimeMs";
    public static final String START_TIME = "StartTime";
    public static final String BEGIN_TXN_TIME_MS = "BeginTxnTimeMs";
    public static final String STREAM_LOAD_PUT_TIME_MS = "StreamLoadPutTimeMs";
    public static final String READ_DATA_TIME_MS = "ReadDataTimeMs";
    public static final String WRITE_DATA_TIME_MS = "WriteDataTimeMs";
    public static final String RECEIVE_DATA_TIME_MS = "ReceiveDataTimeMs";
    public static final String COMMIT_AND_PUBLISH_TIME_MS = "CommitAndPublishTimeMs";
    public static final String ERROR_URL = "ErrorURL";
    public static final String EXISTING_JOB_STATUS = "ExistingJobStatus";

    @AuditField(value = Load_ID)
    public String loadId = "";
    @AuditField(value = TXN_ID)
    public long txnId;
    @AuditField(value = LABEL)
    public String label = "";
    @AuditField(value = COMMENT)
    public String comment = "";
    @AuditField(value = DB)
    public String db = "";
    @AuditField(value = TABLE)
    public String table = "";
    @AuditField(value = USER)
    public String user = "";
    @AuditField(value = CLIENT_IP)
    public String clientIp = "";
    @AuditField(value = STATUS)
    public String status = "";
    @AuditField(value = MESSAGE)
    public String message = "";
    @AuditField(value = ERROR_URL)
    public String errorUrl = "";
    @AuditField(value = NUMBER_TOTAL_ROWS)
    public long totalRows = -1;
    @AuditField(value = NUMBER_LOADED_ROWS)
    public long loadedRows = -1;
    @AuditField(value = NUMBER_FILTERED_ROWS)
    public long filteredRows = -1;
    @AuditField(value = NUMBER_UNSELECTED_ROWS)
    public long unselectedRows = -1;
    @AuditField(value = CPU_COST_MS)
    public long cpuCostMs = -1;
    @AuditField(value = PEAK_USED_MEMORY_BYTES)
    public long peakUsedMemoryBytes = -1;
    @AuditField(value = LOAD_BYTES)
    public long loadBytes = -1;
    @AuditField(value = TWO_PHASE_COMMIT)
    public boolean twoPhaseCommit = false;
    @AuditField(value = GROUP_COMMIT)
    public boolean groupCommit = false;
    @AuditField(value = START_TIME)
    public String startTime = "";
    @AuditField(value = LOAD_TIME_MS)
    public long loadTimeMs = -1;
    @AuditField(value = BEGIN_TXN_TIME_MS)
    public long beginTxnTimeMs = -1;
    @AuditField(value = STREAM_LOAD_PUT_TIME_MS)
    public long streamLoadPutTimeMs = -1;
    @AuditField(value = READ_DATA_TIME_MS)
    public long readDataTimeMs = -1;
    @AuditField(value = WRITE_DATA_TIME_MS)
    public long writeDataTimeMs = -1;
    @AuditField(value = RECEIVE_DATA_TIME_MS)
    public long receiveDataTimeMs = -1;
    @AuditField(value = COMMIT_AND_PUBLISH_TIME_MS)
    public long commitAndPublishTimeMs = -1;
    @AuditField(value = EXISTING_JOB_STATUS)
    public String existingJobStatus = "";
    @AuditField(value = "FinishTime")
    public String finishTime = "";

    public static class AuditEventBuilder {

        private StreamLoadAuditEvent auditEvent = new StreamLoadAuditEvent();

        public AuditEventBuilder() {
        }

        public void reset() {
            auditEvent = new StreamLoadAuditEvent();
        }

        public AuditEventBuilder setEventType(EventType eventType) {
            auditEvent.type = eventType;
            return this;
        }

        public AuditEventBuilder setLabel(String label) {
            auditEvent.label = label;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
            return this;
        }

        public AuditEventBuilder setTable(String table) {
            auditEvent.table = table;
            return this;
        }

        public AuditEventBuilder setUser(String user) {
            auditEvent.user = user;
            return this;
        }

        public AuditEventBuilder setClientIp(String clientIp) {
            auditEvent.clientIp = clientIp;
            return this;
        }

        public AuditEventBuilder setStatus(String status) {
            auditEvent.status = status;
            return this;
        }

        public AuditEventBuilder setMessage(String message) {
            auditEvent.message = message;
            return this;
        }

        public AuditEventBuilder setUrl(String url) {
            auditEvent.errorUrl = url;
            return this;
        }

        public AuditEventBuilder setTotalRows(long totalRows) {
            auditEvent.totalRows = totalRows;
            return this;
        }

        public AuditEventBuilder setLoadedRows(long loadedRows) {
            auditEvent.loadedRows = loadedRows;
            return this;
        }

        public AuditEventBuilder setFilteredRows(long filteredRows) {
            auditEvent.filteredRows = filteredRows;
            return this;
        }

        public AuditEventBuilder setUnselectedRows(long unselectedRows) {
            auditEvent.unselectedRows = unselectedRows;
            return this;
        }

        public AuditEventBuilder setCpuCostMs(long cpuCostMs) {
            auditEvent.cpuCostMs = cpuCostMs;
            return this;
        }

        public AuditEventBuilder setPeakUsedMemoryBytes(long peakUsedMemoryBytes)  {
            auditEvent.peakUsedMemoryBytes = peakUsedMemoryBytes;
            return this;
        }

        public AuditEventBuilder setLoadBytes(long loadBytes) {
            auditEvent.loadBytes = loadBytes;
            return this;
        }

        public AuditEventBuilder setStartTime(String startTime) {
            auditEvent.startTime = startTime;
            return this;
        }

        public AuditEventBuilder setFinishTime(String finishTime) {
            auditEvent.finishTime = finishTime;
            return this;
        }

        public AuditEventBuilder setTwoPhaseCommit(boolean twoPhaseCommit) {
            auditEvent.twoPhaseCommit = twoPhaseCommit;
            return this;
        }

        public AuditEventBuilder setGroupCommit(boolean groupCommit) {
            auditEvent.groupCommit = groupCommit;
            return this;
        }

        public AuditEventBuilder setLoadTimeMs(long loadTimeMs) {
            auditEvent.loadTimeMs = loadTimeMs;
            return this;
        }

        public AuditEventBuilder setBeginTxnTimeMs(long beginTxnTimeMs) {
            auditEvent.beginTxnTimeMs = beginTxnTimeMs;
            return this;
        }

        public AuditEventBuilder setStreamLoadPutTimeMs(long streamLoadPutTimeMs) {
            auditEvent.streamLoadPutTimeMs = streamLoadPutTimeMs;
            return this;
        }

        public AuditEventBuilder setReadDataTimeMs(long readDataTimeMs) {
            auditEvent.readDataTimeMs = readDataTimeMs;
            return this;
        }

        public AuditEventBuilder setWriteDataTimeMs(long writeDataTimeMs) {
            auditEvent.writeDataTimeMs = writeDataTimeMs;
            return this;
        }

        public AuditEventBuilder setReceiveDataTimeMs(long receiveDataTimeMs) {
            auditEvent.receiveDataTimeMs = receiveDataTimeMs;
            return this;
        }

        public AuditEventBuilder setCommitAndPublishTimeMs(long commitAndPublishTimeMs) {
            auditEvent.commitAndPublishTimeMs = commitAndPublishTimeMs;
            return this;
        }

        public AuditEventBuilder setExistingJobStatus(String existingJobStatus) {
            auditEvent.existingJobStatus = existingJobStatus;
            return this;
        }

        public AuditEventBuilder setTxnId(long txnId) {
            auditEvent.txnId = txnId;
            return this;
        }

        public AuditEventBuilder setComment(String comment) {
            auditEvent.comment = comment;
            return this;
        }

        public AuditEventBuilder setLoadId(String loadId) {
            auditEvent.loadId = loadId;
            return this;
        }

        public AuditEvent build() {
            return this.auditEvent;
        }
    }
}
