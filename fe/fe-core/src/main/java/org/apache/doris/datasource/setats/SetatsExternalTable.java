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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.Schema;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.types.Type;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.types.Types;
import com.tencent.oceanus.table.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class SetatsExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(SetatsExternalTable.class);
    private static final int SETATS_DATETIME_SCALE_MS = 6;
    private Table originTable = null;

    public SetatsExternalTable(long id, String name, String remoteName,
            SetatsExternalCatalog catalog, SetatsExternalDatabase db)  {
        super(id, name, remoteName, catalog, db, TableType.SETATS_EXTERNAL_TABLE);
    }

    public String getCatalogType() {
        return ((SetatsExternalCatalog) catalog).getCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
            schemaUpdateTime = System.currentTimeMillis();
            originTable = Env.getCurrentEnv()
                    .getExtMetaCacheMgr()
                    .getSetatsMetadataCache()
                    .getSetatsTable(catalog, dbName, name);
        }
    }

    @Override
    public long fetchRowCount() {
        return UNKNOWN_ROW_COUNT;
    }

    @Override
    public TTableDescriptor toThrift() {
        // todo 是否需要增加 thrift TSetatsTable 类型, 还是跟paimon一样都用 THiveTable.
        List<Column> schema = getFullSchema();
        if (getCatalogType().equals(SetatsExternalCatalog.SETATS_HMS) || getCatalogType().equals(
                SetatsExternalCatalog.SETATS_HADOOP)) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException("Currently only supports hms/hadoop catalog,not support :"
                    + getCatalogType());
        }
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        return HiveMetaStoreClientHelper.ugiDoAs(catalog.getConfiguration(), () -> {
            Table setatsTable = getSetatsTable();
            Schema schema = setatsTable.schema();
            List<Types.NestedField> columns = schema.columns();
            List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
            for (Types.NestedField field : columns) {
                tmpSchema.add(new Column(field.name().toLowerCase(Locale.ROOT),
                        setatsTypeToDorisType(field.type()), true, null, true, field.doc(), true,
                        schema.caseInsensitiveFindField(field.name()).fieldId()));
            }
            return Optional.of(new SchemaCacheValue(tmpSchema));
        });
    }

    public Table getSetatsTable() {
        makeSureInitialized();
        return originTable;
    }

    private static org.apache.doris.catalog.Type setatsPrimitiveTypeToDorisType(Type.PrimitiveType primitive) {
        switch (primitive.typeId()) {
            case BOOLEAN:
                return org.apache.doris.catalog.Type.BOOLEAN;
            case INTEGER:
                return org.apache.doris.catalog.Type.INT;
            case LONG:
                return org.apache.doris.catalog.Type.BIGINT;
            case FLOAT:
                return org.apache.doris.catalog.Type.FLOAT;
            case DOUBLE:
                return org.apache.doris.catalog.Type.DOUBLE;
            case STRING:
            case BINARY:
            case UUID:
                return org.apache.doris.catalog.Type.STRING;
            case FIXED:
                Types.FixedType fixed = (Types.FixedType) primitive;
                return ScalarType.createCharType(fixed.length());
            case DECIMAL:
                Types.DecimalType decimal
                        = (Types.DecimalType) primitive;
                return ScalarType.createDecimalV3Type(decimal.precision(), decimal.scale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP:
                return ScalarType.createDatetimeV2Type(SETATS_DATETIME_SCALE_MS);
            case TIME:
                return org.apache.doris.catalog.Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + primitive);
        }
    }

    public static org.apache.doris.catalog.Type setatsTypeToDorisType(Type type) {
        if (type.isPrimitiveType()) {
            return setatsPrimitiveTypeToDorisType((Type.PrimitiveType) type);
        }
        switch (type.typeId()) {
            case LIST:
                Types.ListType list = (Types.ListType) type;
                return ArrayType.create(setatsTypeToDorisType(list.elementType()), true);
            case MAP:
            case STRUCT:
                return org.apache.doris.catalog.Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + type);
        }
    }
}
