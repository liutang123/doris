--
-- Current Database: `ssb_100g`
--

CREATE DATABASE IF NOT EXISTS `ssb_100g`;

USE `ssb_100g`;

--
-- Table structure for table `customer`
--

CREATE TABLE `customer` (
  `c_custkey` int(11) NOT NULL,
  `c_name` varchar(26) NOT NULL,
  `c_address` varchar(41) NOT NULL,
  `c_city` varchar(11) NOT NULL,
  `c_nation` varchar(16) NOT NULL,
  `c_region` varchar(13) NOT NULL,
  `c_phone` varchar(16) NOT NULL,
  `c_mktsegment` varchar(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);;

--
-- Table structure for table `dates`
--

CREATE TABLE `dates` (
  `d_datekey` int(11) NOT NULL,
  `d_date` varchar(20) NOT NULL,
  `d_dayofweek` varchar(10) NOT NULL,
  `d_month` varchar(11) NOT NULL,
  `d_year` int(11) NOT NULL,
  `d_yearmonthnum` int(11) NOT NULL,
  `d_yearmonth` varchar(9) NOT NULL,
  `d_daynuminweek` int(11) NOT NULL,
  `d_daynuminmonth` int(11) NOT NULL,
  `d_daynuminyear` int(11) NOT NULL,
  `d_monthnuminyear` int(11) NOT NULL,
  `d_weeknuminyear` int(11) NOT NULL,
  `d_sellingseason` varchar(14) NOT NULL,
  `d_lastdayinweekfl` int(11) NOT NULL,
  `d_lastdayinmonthfl` int(11) NOT NULL,
  `d_holidayfl` int(11) NOT NULL,
  `d_weekdayfl` int(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`d_datekey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);;

--
-- Table structure for table `lineorder`
--

CREATE TABLE `lineorder` (
  `lo_orderkey` int(11) NOT NULL,
  `lo_linenumber` int(11) NOT NULL,
  `lo_custkey` int(11) NOT NULL,
  `lo_partkey` int(11) NOT NULL,
  `lo_suppkey` int(11) NOT NULL,
  `lo_orderdate` int(11) NOT NULL,
  `lo_orderpriority` varchar(16) NOT NULL,
  `lo_shippriority` int(11) NOT NULL,
  `lo_quantity` int(11) NOT NULL,
  `lo_extendedprice` int(11) NOT NULL,
  `lo_ordtotalprice` int(11) NOT NULL,
  `lo_discount` int(11) NOT NULL,
  `lo_revenue` int(11) NOT NULL,
  `lo_supplycost` int(11) NOT NULL,
  `lo_tax` int(11) NOT NULL,
  `lo_commitdate` int(11) NOT NULL,
  `lo_shipmode` varchar(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT 'OLAP'
PARTITION BY RANGE(`lo_orderdate`)
(PARTITION p1 VALUES [("-2147483648"), ("19930101")),
PARTITION p2 VALUES [("19930101"), ("19940101")),
PARTITION p3 VALUES [("19940101"), ("19950101")),
PARTITION p4 VALUES [("19950101"), ("19960101")),
PARTITION p5 VALUES [("19960101"), ("19970101")),
PARTITION p6 VALUES [("19970101"), ("19980101")),
PARTITION p7 VALUES [("19980101"), ("19990101")))
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);;

--
-- Table structure for table `part`
--

CREATE TABLE `part` (
  `p_partkey` int(11) NOT NULL,
  `p_name` varchar(23) NOT NULL,
  `p_mfgr` varchar(7) NOT NULL,
  `p_category` varchar(8) NOT NULL,
  `p_brand` varchar(10) NOT NULL,
  `p_color` varchar(12) NOT NULL,
  `p_type` varchar(26) NOT NULL,
  `p_size` int(11) NOT NULL,
  `p_container` varchar(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);;

--
-- Table structure for table `supplier`
--

CREATE TABLE `supplier` (
  `s_suppkey` int(11) NOT NULL,
  `s_name` varchar(26) NOT NULL,
  `s_address` varchar(26) NOT NULL,
  `s_city` varchar(11) NOT NULL,
  `s_nation` varchar(16) NOT NULL,
  `s_region` varchar(13) NOT NULL,
  `s_phone` varchar(16) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);;

