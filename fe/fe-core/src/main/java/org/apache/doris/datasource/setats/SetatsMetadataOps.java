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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.datasource.operations.ExternalMetadataOps;

import com.tencent.oceanus.setats.shaded.org.apache.iceberg.catalog.Catalog;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.catalog.Namespace;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.catalog.SupportsNamespaces;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class SetatsMetadataOps implements ExternalMetadataOps {

    private static final Logger LOG = LogManager.getLogger(SetatsMetadataOps.class);
    protected Catalog catalog;
    protected SetatsExternalCatalog dorisCatalog;
    protected SupportsNamespaces nsCatalog;
    private PreExecutionAuthenticator preExecutionAuthenticator;
    // Generally, there should be only two levels under the catalog, namely <database>.<table>,
    // but the REST type catalog is obtained from an external server,
    // and the level provided by the external server may be three levels, <catalog>.<database>.<table>.
    // Therefore, if the external server provides a catalog,
    // the catalog needs to be recorded here to ensure semantic consistency.
    private Optional<String> externalCatalogName = Optional.empty();

    public SetatsMetadataOps(SetatsExternalCatalog dorisCatalog, Catalog catalog) {
        this.dorisCatalog = dorisCatalog;
        this.catalog = catalog;
        nsCatalog = (SupportsNamespaces) catalog;
        this.preExecutionAuthenticator = dorisCatalog.getPreExecutionAuthenticator();
        if (dorisCatalog.getProperties().containsKey(SetatsExternalCatalog.EXTERNAL_CATALOG_NAME)) {
            externalCatalogName =
                    Optional.of(dorisCatalog.getProperties().get(SetatsExternalCatalog.EXTERNAL_CATALOG_NAME));
        }
    }

    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public void close() {
    }

    private <T> T execute(Callable<T> task) {
        try {
            return preExecutionAuthenticator.execute(task);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        return execute(() -> catalog.tableExists(getTableIdentifier(dbName, tblName)));
    }

    public boolean databaseExist(String dbName) {
        return execute(() -> nsCatalog.namespaceExists(getNamespace(dbName)));
    }

    public List<String> listDatabaseNames() {
        return execute(() -> nsCatalog.listNamespaces(getNamespace())
                    .stream()
                    .map(n -> n.level(n.length() - 1))
                    .collect(Collectors.toList()));
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return execute(() ->
            catalog.listTables(Namespace.of(dbName)).stream()
                .map(TableIdentifier::name).collect(Collectors.toList()));
    }

    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        execute(() -> {
            performCreateDb(stmt);
            return null;
        });
    }

    private void performCreateDb(CreateDbStmt stmt) throws DdlException {
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        String dbName = stmt.getFullDbName();
        if (databaseExist(dbName)) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create database[{}] which already exists", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);
            }
        }
        Map<String, String> properties = stmt.getProperties();
        nsCatalog.createNamespace(Namespace.of(dbName), properties);
        dorisCatalog.onRefreshCache(true);
    }

    @Override
    public void dropDb(DropDbStmt stmt) throws DdlException {
        execute(() -> {
            performDropDb(stmt);
            return null;
        });
    }

    private void performDropDb(DropDbStmt stmt) throws DdlException {
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        String dbName = stmt.getDbName();
        if (!databaseExist(dbName)) {
            if (stmt.isSetIfExists()) {
                LOG.info("drop database[{}] which does not exist", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }
        nsCatalog.dropNamespace(Namespace.of(dbName));
        dorisCatalog.onRefreshCache(true);
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        throw new UnsupportedOperationException("Not support create table.");
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        throw new UnsupportedOperationException("Not support drop table.");
    }

    @Override
    public void truncateTable(String dbName, String tblName, List<String> partitions) {
        throw new UnsupportedOperationException("Truncate Iceberg table is not supported.");
    }

    private TableIdentifier getTableIdentifier(String dbName, String tblName) {
        return externalCatalogName
                .map(s -> TableIdentifier.of(s, dbName, tblName))
                .orElseGet(() -> TableIdentifier.of(dbName, tblName));
    }

    private Namespace getNamespace(String dbName) {
        return externalCatalogName
                .map(s -> Namespace.of(s, dbName))
                .orElseGet(() -> Namespace.of(dbName));
    }

    private Namespace getNamespace() {
        return externalCatalogName.map(
                Namespace::of).orElseGet(() -> Namespace.empty());
    }
}
