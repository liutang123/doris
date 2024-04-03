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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum MTLogger {

    DORIS_AUDIT("doris_audit"),
    DORIS_AUDIT_EVENT("doris_audit_event"),
    DORIS_PROFILE("doris_new_fe_profile"),
    DORIS_AUDIT_AGENT_TASK("doris_audit_agent_task"),
    ;

    public final String name;
    public final Logger logger;
    public final boolean socket;

    MTLogger(String name) {
        this(name, false);
    }

    MTLogger(String name, boolean socket) {
        this.name = name;
        this.logger = LoggerFactory.getLogger(name);
        this.socket = socket;
    }
}
