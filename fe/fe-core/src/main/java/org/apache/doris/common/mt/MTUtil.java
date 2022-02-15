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

import com.sankuai.xm.pub.push.Pusher;
import com.sankuai.xm.pub.push.PusherBuilder;

public final class MTUtil {

    private static final String CLIENT_ID = "0812090313119182";
    private static final String CLIENT_SECRET = "64e91cae57d9be0461413845f040599d";
    private static final String PUB_FULL_URL = "https://xmapi.vip.sankuai.com/api/pub/push";
    private static final Long FROM_UID = 137443448803L;

    public static Pusher getXmPusher() {
        return PusherBuilder.defaultBuilder()
                .withAppkey(CLIENT_ID)
                .withApptoken(CLIENT_SECRET)
                .withTargetUrl(PUB_FULL_URL)
                .withFromUid(FROM_UID).build();
    }

}