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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

/**
 * switch database
 */
public class UseDatabaseCommand extends Command implements NoForward {
    private final String catalog;
    private final String database;

    public UseDatabaseCommand(String catalog, String database) {
        super(PlanType.USE_DATABASE_COMMAND);
        this.catalog = catalog;
        this.database = database;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUseDatabaseCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (Strings.isNullOrEmpty(database)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }

        String previousCatalogName = ctx.getDefaultCatalog();
        if (!Strings.isNullOrEmpty(this.catalog) && !this.catalog.equals(previousCatalogName)) {
            ctx.getEnv().changeCatalog(ctx, this.catalog);
        }
        try {
            ctx.getEnv().changeDb(ctx, database);
        } catch (Exception e) {
            ctx.getEnv().changeCatalog(ctx, previousCatalogName);
            throw e;
        }
    }

    @Override
    public StmtType stmtType() {
        return StmtType.USE;
    }
}
