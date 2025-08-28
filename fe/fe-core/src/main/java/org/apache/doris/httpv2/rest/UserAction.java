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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.entity.RestBaseResult;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.User;
import org.apache.doris.qe.ConnectContext;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class UserAction extends RestBaseController {
    protected static final String USER_KEY = "user";

    @RequestMapping(path = "/api/{" + USER_KEY + "}/_password", method = RequestMethod.GET)
    public Object getUserInfo(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable(value = USER_KEY) String identity) {
        try {
            checkWithCookie(request, response, false);
            ConnectContext context = ConnectContext.getIfExists();
            if (context == null) {
                return new RestBaseResult("Auth success but some errors occurs.");
            }
            checkGlobalAuth(context.getCurrentUserIdentity(), PrivPredicate.OPERATOR);

            UserIdentity userIdentity = UserIdentity.fromString(identity);
            if (userIdentity == null) {
                return new RestBaseResult("Invalid user identity: " + identity);
            }
            User user = Env.getCurrentEnv().getAuth().getCopiedUserByIdentity(userIdentity);
            if (user == null) {
                return new RestBaseResult("Can not find user: " + identity);
            }
            Map<String, String> res = new HashMap<>();
            if (user.hasPassword()) {
                res.put("password", new String(user.getPassword().getPassword()));
                res.put("hasPass", Boolean.toString(true));
            } else {
                res.put("password", "");
                res.put("hasPass", Boolean.toString(false));
            }
            return ResponseEntityBuilder.ok(res);
        } finally {
            ConnectContext.remove();
        }
    }
}
