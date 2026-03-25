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
suite("eliminate_grouping_sets_by_filter") {
sql """
CREATE TABLE `positions` (
  `address` varchar(65533) NULL,
  `coin` varchar(65533) NULL,
  `dex` varchar(65533) NULL,
  `amount` int NULL
) ENGINE=OLAP
DUPLICATE KEY(`address`)
DISTRIBUTED BY HASH(`address`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
"""
    sql """
INSERT INTO positions VALUES
    ('abc', 'btc', 'uniswap', 10),
    ('abc', 'btc', 'sushiswap', 20),
    ('abc', 'eth', 'uniswap', 5),
    ('def', 'btc', 'uniswap', 7),
    ('def', 'eth', 'sushiswap', 3);
"""
    sql """
CREATE VIEW positions_coin AS
   SELECT
       address,
       coin,
       dex,
       SUM(amount) AS total_amount
    FROM positions
    GROUP BY address, coin, dex;
"""
    sql """
CREATE VIEW positions_dex AS
     SELECT
         address,
         dex,
         SUM(total_amount) AS total_amount
     FROM positions_coin
     GROUP BY CUBE(address, dex);
"""
    // 测试用例 0
explainAndResult 'normal_eliminate', """
SELECT * FROM positions_dex WHERE dex IS NULL   AND address = 'abc';
"""

// 测试用例1: 使用聚合维度中包含函数的，例如concat
explainAndResult 'function_in_grouping_sets', """
SELECT 
    address,
    CONCAT(coin, '_', dex) as coin_dex,
    SUM(total_amount) as total_amount
FROM positions_coin
GROUP BY GROUPING SETS ((address), (CONCAT(coin, '_', dex)))
HAVING CONCAT(coin, '_', dex) = 'btc_uniswap';
"""

    explainAndResult 'function_in_grouping_sets', """
SELECT 
    address,
    CONCAT(coin, '_', dex) as coin_dex,
    SUM(total_amount) as total_amount
FROM positions_coin
GROUP BY GROUPING SETS ((address), (CONCAT(coin, '_', dex)))
HAVING address = 'abc' AND CONCAT(coin, '_', dex) = 'btc_uniswap';
"""

// 测试用例2: 使用cube的
explainAndResult 'cube_elimination', """
SELECT 
    address,
    coin,
    dex,
    SUM(total_amount) as total_amount
FROM positions_coin
GROUP BY CUBE(address, coin, dex)
WHERE address = 'abc' AND dex = 'uniswap';
"""

// 测试用例3: 使用rollup的
explainAndResult 'rollup_elimination', """
SELECT 
    address,
    coin,
    dex,
    SUM(total_amount) as total_amount
FROM positions_coin
GROUP BY ROLLUP(address, coin, dex)
WHERE address = 'abc' AND coin = 'btc' AND dex = 'uniswap';
"""

// 测试用例4: 直接查询positions的
explainAndResult 'direct_positions_query', """
SELECT 
    address,
    coin,
    dex,
    SUM(amount) as total_amount
FROM positions
GROUP BY GROUPING SETS ((address), (coin), (dex))
WHERE address = 'abc' AND coin = 'btc' AND dex = 'uniswap';
"""
// 测试用例5: 查询positions_coin的
    explainAndResult 'positions_coin_query', """
SELECT 
    address,
    coin,
    dex,
    SUM(total_amount) as total_amount
FROM positions_coin
GROUP BY GROUPING SETS ((address, coin), (dex))
WHERE address = 'abc' AND coin = 'btc';
"""

// 测试用例6: 查询positions_coin且过滤条件可以完全把grouping sets过滤掉的
explainAndResult 'complete_filter_elimination', """
SELECT 
    address,
    coin,
    dex,
    SUM(total_amount) as total_amount
FROM positions_coin
GROUP BY GROUPING SETS ((address), (coin), (dex))
WHERE address = 'abc' AND coin = 'btc' AND dex = 'uniswap';
"""
}