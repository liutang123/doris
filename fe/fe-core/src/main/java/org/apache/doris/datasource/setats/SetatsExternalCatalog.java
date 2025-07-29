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

package org.apache.doris.datasource.setats;

import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import com.tencent.oceanus.setats.shaded.org.apache.iceberg.catalog.Catalog;

import java.util.List;

public abstract class SetatsExternalCatalog extends ExternalCatalog {

    public static final String SETATS_CATALOG_TYPE = "setats.catalog.type";
    public static final String SETATS_HMS = "hms";
    public static final String SETATS_HADOOP = "hadoop";
    public static final String EXTERNAL_CATALOG_NAME = "external_catalog.name";
    protected String setatsCatalogType;
    protected Catalog catalog;

    public SetatsExternalCatalog(long catalogId, String name, String comment) {
        super(catalogId, name, InitCatalogLog.Type.SETATS, comment);
    }

    // Create catalog based on catalog type
    protected abstract Catalog initCatalog();

    public Catalog getCatalog() {
        makeSureInitialized();
        return ((SetatsMetadataOps) metadataOps).getCatalog();
    }

    public String getCatalogType() {
        makeSureInitialized();
        return setatsCatalogType;
    }

    @Override
    protected List<String> listDatabaseNames() {
        makeSureInitialized();
        return metadataOps.listDatabaseNames();
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return metadataOps.listTableNames(dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return metadataOps.tableExist(dbName, tblName);
    }

    @Override
    protected void initLocalObjectsImpl() {
        preExecutionAuthenticator = new PreExecutionAuthenticator();
        AuthenticationConfig config = AuthenticationConfig.getKerberosConfig(getConfiguration());
        HadoopAuthenticator authenticator = HadoopAuthenticator.getHadoopAuthenticator(config);
        preExecutionAuthenticator.setHadoopAuthenticator(authenticator);
        try {
            catalog = preExecutionAuthenticator.execute(this::initCatalog);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        metadataOps = new SetatsMetadataOps(this, catalog);
    }
}
