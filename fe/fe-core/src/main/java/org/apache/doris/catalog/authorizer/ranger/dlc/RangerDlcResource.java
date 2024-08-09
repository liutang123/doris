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

package org.apache.doris.catalog.authorizer.ranger.dlc;

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.Objects;
import java.util.Optional;

public class RangerDlcResource extends RangerAccessResourceImpl {
    public static final String KEY_CATALOG = "catalog";
    public static final String KEY_SCHEMA = "schema";
    public static final String KEY_TABLE = "table";
    public static final String KEY_COLUMN = "column";
    public static final String KEY_USER = "prestouser";
    public static final String KEY_FUNCTION = "function";
    public static final String KEY_PROCEDURE = "procedure";
    public static final String KEY_SYSTEM_PROPERTY = "systemproperty";
    public static final String KEY_SESSION_PROPERTY = "sessionproperty";

    public static final String DEFAULT_CATALOG = "DataLakeCatalog";

    public RangerDlcResource() {}

    public RangerDlcResource(String catalog, Optional<String> schema, Optional<String> table, Optional<String> col) {
        Objects.requireNonNull(catalog, "catalog must be not null");
        setValue(KEY_CATALOG, DEFAULT_CATALOG);
        schema.ifPresent(s -> setValue(KEY_SCHEMA, s));
        table.ifPresent(s -> setValue(KEY_TABLE, s));
        col.ifPresent(s -> setValue(KEY_COLUMN, s));
    }

    public static RangerDlcResource of(String ctl) {
        return new RangerDlcResource(ctl, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static RangerDlcResource of(String ctl, String schema) {
        return new RangerDlcResource(ctl, Optional.of(schema), Optional.empty(), Optional.empty());
    }

    public static RangerDlcResource of(String ctl, String schema, String table) {
        return new RangerDlcResource(ctl, Optional.of(schema), Optional.of(table), Optional.empty());
    }

    public static RangerDlcResource of(String ctl, String schema, String table, String col) {
        return new RangerDlcResource(ctl, Optional.of(schema), Optional.of(table), Optional.of(col));
    }

    public static RangerDlcResource createUserResource(String userName) {
        RangerDlcResource res = new RangerDlcResource();
        res.setValue(RangerDlcResource.KEY_USER, userName);
        return res;
    }

    public static RangerDlcResource createFunctionResource(String function) {
        RangerDlcResource res = new RangerDlcResource();
        res.setValue(RangerDlcResource.KEY_FUNCTION, function);
        return res;
    }

    public static RangerDlcResource createProcedureResource(String catalog, String schema, String procedure) {
        RangerDlcResource res = new RangerDlcResource();
        res.setValue(RangerDlcResource.KEY_CATALOG, catalog);
        res.setValue(RangerDlcResource.KEY_SCHEMA, schema);
        res.setValue(RangerDlcResource.KEY_PROCEDURE, procedure);
        return res;
    }

    public static RangerDlcResource createCatalogSessionResource(String catalogName, String propertyName) {
        RangerDlcResource res = new RangerDlcResource();
        res.setValue(RangerDlcResource.KEY_CATALOG, catalogName);
        res.setValue(RangerDlcResource.KEY_SESSION_PROPERTY, propertyName);
        return res;
    }

    public static RangerDlcResource createSystemPropertyResource(String property) {
        RangerDlcResource res = new RangerDlcResource();
        res.setValue(RangerDlcResource.KEY_SYSTEM_PROPERTY, property);
        return res;
    }
}
