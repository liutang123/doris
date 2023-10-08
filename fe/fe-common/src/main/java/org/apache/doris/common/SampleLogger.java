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

package org.apache.doris.common;

import java.util.Random;
import org.apache.logging.log4j.Logger;

public class SampleLogger {
    private static final Random r = new Random();

    public static void sampleInfoLog(Logger logger ,String format, Object... argument) {
        if (sample()) {
            logger.info("["+Thread.currentThread().getStackTrace()[2].getFileName()+"."+Thread.currentThread().getStackTrace()[2].getMethodName()+"():"+Thread.currentThread().getStackTrace()[2].getLineNumber()+"]"+format,argument);
        }
    }

    public static void sampleDebugLog(Logger logger ,String format, Object... argument) {
        if (sample()) {
            logger.debug("["+Thread.currentThread().getStackTrace()[2].getFileName()+"."+Thread.currentThread().getStackTrace()[2].getMethodName()+"():"+Thread.currentThread().getStackTrace()[2].getLineNumber()+"]"+format,argument);
        }
    }

    public static void sampleWarnLog(Logger logger ,String format, Object... argument) {
        if (sample()) {
            logger.warn("["+Thread.currentThread().getStackTrace()[2].getFileName()+"."+Thread.currentThread().getStackTrace()[2].getMethodName()+"():"+Thread.currentThread().getStackTrace()[2].getLineNumber()+"]"+format,argument);
        }
    }

    public static void sampleErrorLog(Logger logger ,String format, Object... argument) {
        if (sample()) {
            logger.error("["+Thread.currentThread().getStackTrace()[2].getFileName()+"."+Thread.currentThread().getStackTrace()[2].getMethodName()+"():"+Thread.currentThread().getStackTrace()[2].getLineNumber()+"]"+format,argument);
        }
    }

    private static boolean sample() {
        int sampleNum = r.nextInt(10000);
        if (sampleNum <= Config.sample_log_rate) {
            return true;
        } else {
            return false;
        }
    }
}