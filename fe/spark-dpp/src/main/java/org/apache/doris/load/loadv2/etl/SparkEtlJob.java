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

package org.apache.doris.load.loadv2.etl;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.dpp.GlobalDictBuilder;
import org.apache.doris.load.loadv2.dpp.SparkDpp;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlTable;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SparkEtlJob is responsible for global dict building, data partition, data sort and data aggregation.
 * 1. init job config
 * 2. check if job has bitmap_dict function columns
 * 3. build global dict if step 2 is true
 * 4. dpp (data partition, data sort and data aggregation)
 */
public class SparkEtlJob {
    private static final Logger LOG = LogManager.getLogger(SparkEtlJob.class);

    private static final String BITMAP_DICT_FUNC = "bitmap_dict";
    private static final String TO_BITMAP_FUNC = "to_bitmap";

    private static final String MT_HDFS_PREFIX = "hdfs://dfsrouter.vip.sankuai.com:8888";
    private static final String BITMAP_HASH = "bitmap_hash";

    private static final Splitter SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();
    private static final String BINARY_BITMAP = "binary_bitmap";

    private String jobConfigFilePath;
    private EtlJobConfig etlJobConfig;
    private Set<Long> hiveSourceTables;
    private Map<Long, Set<String>> tableToBitmapDictColumns;
    private Map<Long, Set<String>> tableToBinaryBitmapColumns;
    private final SparkConf conf;
    private SparkSession spark;
    private String globalDictTableName;
    private String distinctKeyTableName;
    private String dorisIntermediateHiveTable;
    private List<String> tempTables;

    private SparkEtlJob(String jobConfigFilePath) {
        this.jobConfigFilePath = jobConfigFilePath;
        this.etlJobConfig = null;
        this.hiveSourceTables = Sets.newHashSet();
        this.tableToBitmapDictColumns = Maps.newHashMap();
        this.tableToBinaryBitmapColumns = Maps.newHashMap();
        conf = new SparkConf();
        this.tempTables = Lists.newArrayList();
    }

    private void initSpark() {
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.doris.load.loadv2.dpp.DorisKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "false");
        spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();
    }

    private void initSparkConfigs(Map<String, String> configs) {
        if (configs == null) {
            return;
        }
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
            conf.set("spark.hadoop." + entry.getKey(), entry.getValue());
        }
    }

    private void initConfig() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("job config file path: " + jobConfigFilePath);
        }
        Configuration hadoopConf = SparkHadoopUtil.get().newConfiguration(this.conf);
        String jsonConfig;
        Path path = new Path(jobConfigFilePath);
        try (FileSystem fs = path.getFileSystem(hadoopConf); DataInputStream in = fs.open(path)) {
            jsonConfig = CharStreams.toString(new InputStreamReader(in));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("rdd read json config: " + jsonConfig);
        }
        etlJobConfig = EtlJobConfig.configFromJson(jsonConfig);
        etlJobConfig.outputPath = etlJobConfig.outputPath.replace(MT_HDFS_PREFIX, "");
        LOG.info("etl job config: " + etlJobConfig);
    }

    /*
     * 1. check bitmap column
     * 2. fill tableToBitmapDictColumns
     * 3. remove bitmap_dict and to_bitmap mapping from columnMappings
     */
    private void checkConfig() throws Exception {
        for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
            boolean isHiveSource = false;
            Set<String> bitmapDictColumns = Sets.newHashSet();
            Set<String> binaryBitmapColumns = Sets.newHashSet();

            for (EtlFileGroup fileGroup : entry.getValue().fileGroups) {
                if (fileGroup.sourceType == EtlJobConfig.SourceType.HIVE) {
                    isHiveSource = true;
                }
                Map<String, EtlColumnMapping> newColumnMappings = Maps.newHashMap();
                for (Map.Entry<String, EtlColumnMapping> mappingEntry : fileGroup.columnMappings.entrySet()) {
                    String columnName = mappingEntry.getKey();
                    String exprStr = mappingEntry.getValue().toDescription();
                    String funcName = functions.expr(exprStr).expr().prettyName();
                    if (funcName.equalsIgnoreCase(BITMAP_HASH)) {
                        throw new SparkDppException("spark load not support bitmap_hash now");
                    }
                    if (funcName.equalsIgnoreCase(BINARY_BITMAP)) {
                        binaryBitmapColumns.add(columnName.toLowerCase());
                    } else if (funcName.equalsIgnoreCase(BITMAP_DICT_FUNC)) {
                        bitmapDictColumns.add(columnName.toLowerCase());
                    } else if (!funcName.equalsIgnoreCase(TO_BITMAP_FUNC)) {
                        newColumnMappings.put(mappingEntry.getKey(), mappingEntry.getValue());
                    }
                }
                // reset new columnMappings
                fileGroup.columnMappings = newColumnMappings;
            }
            if (isHiveSource) {
                hiveSourceTables.add(entry.getKey());
            }
            if (!bitmapDictColumns.isEmpty()) {
                tableToBitmapDictColumns.put(entry.getKey(), bitmapDictColumns);
            }
            if (!binaryBitmapColumns.isEmpty()) {
                tableToBinaryBitmapColumns.put(entry.getKey(), binaryBitmapColumns);
            }
        }
        LOG.info("init hiveSourceTables: " + hiveSourceTables
                + ",tableToBitmapDictColumns: " + tableToBitmapDictColumns);

        // spark etl must have only one table with bitmap type column to process.
        if (hiveSourceTables.size() > 1
                || tableToBitmapDictColumns.size() > 1
                || tableToBinaryBitmapColumns.size() > 1) {
            throw new Exception("spark etl job must have only one hive table with bitmap type column to process");
        }
    }

    private void processDpp() throws Exception {
        SparkDpp sparkDpp = new SparkDpp(spark, etlJobConfig, tableToBitmapDictColumns, tableToBinaryBitmapColumns);
        sparkDpp.init();
        sparkDpp.doDpp();
    }

    private String buildGlobalDictAndEncodeSourceTable(EtlTable table, long tableId) {
        // dict column map
        MultiValueMap dictColumnMap = new MultiValueMap();
        for (String dictColumn : tableToBitmapDictColumns.get(tableId)) {
            dictColumnMap.put(dictColumn, null);
        }

        // doris schema
        List<String> dorisOlapTableColumnList = Lists.newArrayList();
        for (EtlIndex etlIndex : table.indexes) {
            if (etlIndex.isBaseIndex) {
                for (EtlColumn column : etlIndex.columns) {
                    dorisOlapTableColumnList.add(column.columnName);
                }
            }
        }

        // hive db and tables
        EtlFileGroup fileGroup = table.fileGroups.get(0);
        String sourceHiveDBTableName = fileGroup.hiveDbTableName;

        String dorisHiveDB = "mart_doris";
        if (!Strings.isNullOrEmpty(etlJobConfig.customizedProperties.getOrDefault("custom.hive.db", ""))) {
            dorisHiveDB = etlJobConfig.customizedProperties.get("custom.hive.db");
        }

        String appId = spark.sparkContext().applicationId().replace("application", "");
        // todo(wx):  zookeeper lock is too heavy now globalDict build should be build one by one
        globalDictTableName = String.format(EtlJobConfig.GLOBAL_DICT_TABLE_NAME,
                etlJobConfig.dorisDBName, etlJobConfig.dorisTableName);
        distinctKeyTableName = String.format(EtlJobConfig.DISTINCT_KEY_TABLE_NAME,
                etlJobConfig.dorisDBName, etlJobConfig.dorisTableName, appId);
        dorisIntermediateHiveTable = String.format(EtlJobConfig.DORIS_INTERMEDIATE_HIVE_TABLE_NAME,
                etlJobConfig.dorisDBName, etlJobConfig.dorisTableName, appId);
        String sourceHiveFilter = fileGroup.where;
        // todo(wx): make all magic name of custom prefix in constant.

        globalDictTableName = etlJobConfig.customizedProperties.getOrDefault("custom.global.dict.table", globalDictTableName);
        boolean buildIndependent = Boolean.parseBoolean(etlJobConfig.customizedProperties.getOrDefault("custom.buildGlobalIndependentDict", ""));
        if (buildIndependent) {
            globalDictTableName = globalDictTableName + "_" + spark.sparkContext().applicationId();
            tempTables.add(globalDictTableName);
        }
        tempTables.add(distinctKeyTableName);
        tempTables.add(dorisIntermediateHiveTable);

        // others
        int buildConcurrency = 1; // do not change this
        String skipNullCols = etlJobConfig.customizedProperties.getOrDefault("custom.skipnull.columns", null);
        String veryHighCardinalityCols = etlJobConfig.customizedProperties.getOrDefault("custom.veryhighcardinality.columns", null);
        String veryHighCardColumnSplitNum = etlJobConfig.customizedProperties.getOrDefault("custom.veryhighcolumnsplit.num", "10");
        String mapSideJoinCols = etlJobConfig.customizedProperties.getOrDefault("custom.mapsidejoin.columns", null);

        LOG.info("global dict builder args, dictColumnMap: " + dictColumnMap
                         + ", dorisOlapTableColumnList: " + dorisOlapTableColumnList
                         + ", sourceHiveDBTableName: " + sourceHiveDBTableName
                         + ", sourceHiveFilter: "+ sourceHiveFilter
                         + ", skipNullColumns: " + skipNullCols
                         + ", veryHighCardinalityColumns: " + veryHighCardinalityCols
                         + ", veryHighCardinalityColumnSplitNum: " + veryHighCardColumnSplitNum
                         + ", mapSideJoinColumns: " + mapSideJoinCols
                         + ", distinctKeyTableName: " + distinctKeyTableName
                         + ", globalDictTableName: " + globalDictTableName
                         + ", dorisIntermediateHiveTable: " + dorisIntermediateHiveTable);
        try {
            if (globalDictTableName.length() > 128 || distinctKeyTableName.length() > 128
                    || dorisIntermediateHiveTable.length() > 128) {
                String errorMsg = "internal hive table name is longer than 128. globalDictTableName: "
                            + globalDictTableName
                            + ", distinctKeyTableName: " + distinctKeyTableName
                            + ", dorisIntermediateHiveTable: " + dorisIntermediateHiveTable;
                throw new Exception(errorMsg);
            }

            List<String> skipNullColumn = skipNullCols == null ? Lists.newArrayList() : Lists.newArrayList(SPLITTER.split(skipNullCols));
            List<String> veryHighCardinalityColumn = veryHighCardinalityCols == null ? Lists.newArrayList() : Lists.newArrayList(SPLITTER.split(veryHighCardinalityCols));
            int veryHighCardinalityColumnSplitNum = Integer.parseInt(veryHighCardColumnSplitNum);
            List<String> mapSideJoinColumns = mapSideJoinCols == null ? Lists.newArrayList() : Lists.newArrayList(SPLITTER.split(mapSideJoinCols));
            GlobalDictBuilder globalDictBuilder = new GlobalDictBuilder(
                    dictColumnMap, dorisOlapTableColumnList, mapSideJoinColumns, sourceHiveDBTableName,
                    sourceHiveFilter, dorisHiveDB, distinctKeyTableName, globalDictTableName, dorisIntermediateHiveTable,
                    buildConcurrency, veryHighCardinalityColumn, veryHighCardinalityColumnSplitNum, skipNullColumn, spark);
            globalDictBuilder.createHiveIntermediateTable();
            globalDictBuilder.extractDistinctColumn();
            globalDictBuilder.buildGlobalDict();
            globalDictBuilder.encodeDorisIntermediateHiveTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return String.format("%s.%s", dorisHiveDB, dorisIntermediateHiveTable);
    }

    private void processData() throws Exception {
        if (!hiveSourceTables.isEmpty()) {
            // only one table
            long tableId = -1;
            EtlTable table = null;
            for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
                tableId = entry.getKey();
                table = entry.getValue();
                break;
            }

            // init hive configs like metastore service
            EtlFileGroup fileGroup = table.fileGroups.get(0);
            initSparkConfigs(fileGroup.hiveTableProperties);
            fileGroup.dppHiveDbTableName = fileGroup.hiveDbTableName;

            // build global dict and encode source hive table if has bitmap dict columns
            if (!tableToBitmapDictColumns.isEmpty() && tableToBitmapDictColumns.containsKey(tableId)) {
                String dorisIntermediateHiveDbTableName = buildGlobalDictAndEncodeSourceTable(table, tableId);
                // set with dorisIntermediateHiveDbTable
                fileGroup.dppHiveDbTableName = dorisIntermediateHiveDbTableName;
            }
        }

        initSpark();
        // data partition sort and aggregation
        processDpp();
    }

    private void deleteTmpTable() {
        for (String table : this.tempTables) {
            LOG.info("drop table " + table);
            spark.sql("DROP TABLE IF EXISTS " + table);
        }
    }

    private void run() throws Exception {
        initConfig();
        checkConfig();
        try {
            processData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            deleteTmpTable();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("missing job config file path arg");
            System.exit(-1);
        }

        try {
            new SparkEtlJob(args[0]).run();
        } catch (Exception e) {
            System.err.println("spark etl job run failed");
            LOG.warn("", e);
            System.exit(-1);
        }
    }
}
