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

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.ManifestFiles;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.Snapshot;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.catalog.Catalog;
import com.tencent.oceanus.setats.shaded.org.apache.iceberg.catalog.TableIdentifier;
import com.tencent.oceanus.table.Table;
import com.tencent.oceanus.table.TableAdaptor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

public class SetatsMetadataCache {

    private final LoadingCache<SetatsMetadataCacheKey, List<Snapshot>> snapshotListCache;
    private final LoadingCache<SetatsMetadataCacheKey, Table> tableCache;

    public SetatsMetadataCache(ExecutorService executor) {
        CacheFactory snapshotListCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.snapshotListCache = snapshotListCacheFactory.buildCache(key -> loadSnapshots(key), null, executor);

        CacheFactory tableCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_external_table_cache_num,
                true,
                null);
        this.tableCache = tableCacheFactory.buildCache(key -> loadTable(key), null, executor);
    }

    private static void initSetatsTableFileIO(Table table, Map<String, String> props) {
        Map<String, String> ioConf = new HashMap<>();
        table.properties().forEach((key, value) -> {
            if (key.startsWith("io.")) {
                ioConf.put(key, value);
            }
        });

        // This `initialize` method will directly override the properties as a whole,
        // so we need to merge the table's io-related properties with the doris's catalog-related properties
        props.putAll(ioConf);
        table.io().initialize(props);
    }

    // public List<Snapshot> getSnapshotList(TIcebergMetadataParams params) throws UserException {
    //     CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(params.getCatalog());
    //     if (catalog == null) {
    //         throw new UserException("The specified catalog does not exist:" + params.getCatalog());
    //     }
    //     IcebergMetadataCacheKey key =
    //             IcebergMetadataCacheKey.of(catalog, params.getDatabase(), params.getTable());
    //     return snapshotListCache.get(key);
    // }

    public Table getSetatsTable(CatalogIf catalog, String dbName, String tbName) {
        SetatsMetadataCacheKey key = SetatsMetadataCacheKey.of(catalog, dbName, tbName);
        return tableCache.get(key);
    }

    @NotNull
    private List<Snapshot> loadSnapshots(SetatsMetadataCacheKey key) {
        Table setatsTable = getSetatsTable(key.catalog, key.dbName, key.tableName);
        List<Snapshot> snaps = Lists.newArrayList();
        Iterables.addAll(snaps, setatsTable.snapshots());
        return snaps;
    }

    @NotNull
    private Table loadTable(SetatsMetadataCacheKey key) {
        Catalog setatsCatalog = ((SetatsExternalCatalog) key.catalog).getCatalog();
        try {
            com.tencent.oceanus.setats.shaded.org.apache.iceberg.Table icebergTable =
                    ((ExternalCatalog) key.catalog).getPreExecutionAuthenticator().execute(
                            () -> setatsCatalog.loadTable(
                                    TableIdentifier.of(key.dbName, key.tableName)));
            Table setatsTable = TableAdaptor.createTable(icebergTable);
            initSetatsTableFileIO(setatsTable, key.catalog.getProperties());
            return setatsTable;
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    public void invalidateCatalogCache(long catalogId) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.catalog.getId() == catalogId)
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> entry.getKey().catalog.getId() == catalogId)
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    public void invalidateTableCache(long catalogId, String dbName, String tblName) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.catalog.getId() == catalogId && key.dbName.equals(dbName) && key.tableName.equals(
                        tblName))
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> {
                    SetatsMetadataCacheKey key = entry.getKey();
                    return key.catalog.getId() == catalogId && key.dbName.equals(dbName) && key.tableName.equals(
                            tblName);
                })
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        snapshotListCache.asMap().keySet().stream()
                .filter(key -> key.catalog.getId() == catalogId && key.dbName.equals(dbName))
                .forEach(snapshotListCache::invalidate);

        tableCache.asMap().entrySet().stream()
                .filter(entry -> {
                    SetatsMetadataCacheKey key = entry.getKey();
                    return key.catalog.getId() == catalogId && key.dbName.equals(dbName);
                })
                .forEach(entry -> {
                    ManifestFiles.dropCache(entry.getValue().io());
                    tableCache.invalidate(entry.getKey());
                });
    }

    static class SetatsMetadataCacheKey {
        CatalogIf catalog;
        String dbName;
        String tableName;

        public SetatsMetadataCacheKey(CatalogIf catalog, String dbName, String tableName) {
            this.catalog = catalog;
            this.dbName = dbName;
            this.tableName = tableName;
        }

        static SetatsMetadataCacheKey of(CatalogIf catalog, String dbName, String tableName) {
            return new SetatsMetadataCacheKey(
                    catalog,
                    dbName,
                    tableName
            );
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalog, dbName, tableName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SetatsMetadataCacheKey that = (SetatsMetadataCacheKey) o;
            return catalog == that.catalog
                    && Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName);
        }
    }
}
