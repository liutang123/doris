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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.MustFallbackException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.DorisDataMaskPolicy;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Create policy command use for row policy and storage policy.
 */
public class CreatePolicyCommand extends Command implements ForwardWithSync {

    private final PolicyTypeEnum policyType;
    private final String policyName;
    private final boolean ifNotExists;
    private final List<String> nameParts;
    private final Optional<FilterType> filterType;
    private final UserIdentity user;
    private final String roleName;
    private final Optional<Expression> wherePredicate;
    private final Map<String, String> properties;
    private String dataMaskType;
    private int priority;

    /**
     * ctor of this command.
     */
    public CreatePolicyCommand(PolicyTypeEnum policyType, String policyName, boolean ifNotExists,
            List<String> nameParts, Optional<FilterType> filterType, UserIdentity user, String roleName,
            Optional<Expression> wherePredicate, Map<String, String> properties) {
        super(PlanType.CREATE_POLICY_COMMAND);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifNotExists = ifNotExists;
        this.nameParts = nameParts;
        this.filterType = filterType;
        this.user = user;
        this.roleName = roleName;
        this.wherePredicate = wherePredicate;
        this.properties = properties;
    }

    public CreatePolicyCommand(PolicyTypeEnum policyType, String policyName, boolean ifNotExists,
                               List<String> nameParts, UserIdentity user, String roleName, String dataMaskType,
                               int priority) {
        this(policyType, policyName, ifNotExists, nameParts, Optional.empty(), user, roleName, Optional.empty(),
                ImmutableMap.of());
        this.dataMaskType = dataMaskType;
        this.priority = priority;
    }

    public Optional<Expression> getWherePredicate() {
        return wherePredicate;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreatePolicyCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (policyType == PolicyTypeEnum.DATA_MASK) {
            if (user != null) {
                user.analyze();
                if (user.isRootUser() || user.isAdminUser()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "CreatePolicyStmt",
                            user.getQualifiedUser(), user.getHost(), nameParts.get(0));
                }
            }
            // check auth
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        PrivPredicate.GRANT.getPrivs().toString());
            }
            String ctl = nameParts.get(0);
            String db = nameParts.get(1);
            String tbl = nameParts.get(2);
            String col = nameParts.get(3);
            if (ctl == null || db == null || tbl == null || col == null) {
                throw new org.apache.doris.common.AnalysisException("ctl, db, tbl and col should not be null");
            }
            Column column = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(ctl)
                    .getDbOrAnalysisException(db)
                    .getTableOrAnalysisException(tbl).getColumn(col);
            if (column == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, col, tbl);
            }
            if (roleName != null) {
                boolean roleExists = Env.getCurrentEnv().getAuth().doesRoleExist(roleName);
                if (!roleExists) {
                    throw new DdlException("Role: " + roleName + " does not exist");
                }
            }
            if (user != null) {
                boolean userExists = Env.getCurrentEnv().getAuth().doesUserExist(user);
                if (!userExists) {
                    throw new DdlException("User: " + user + " does not exist");
                }
            }
            Policy policy = new DorisDataMaskPolicy(Env.getCurrentEnv().getNextId(), policyName, user, roleName,
                    ctl, db, tbl, col, dataMaskType, priority);
            Env.getCurrentEnv().getPolicyMgr().createPolicy(policy, ifNotExists);
            return;
        }
        throw new MustFallbackException("Not support create policy command in Nereids now");
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
