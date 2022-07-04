--
-- Current Database: `tpch_100g`
--

CREATE DATABASE IF NOT EXISTS `tpch_100g`;

USE `tpch_100g`;

--
-- Table structure for table `customer`
--

CREATE TABLE IF NOT EXISTS `customer` (
  `c_custkey` int(11) NOT NULL,
  `c_name` varchar(25) NOT NULL,
  `c_address` varchar(40) NOT NULL,
  `c_nationkey` int(11) NOT NULL,
  `c_phone` varchar(15) NOT NULL,
  `c_acctbal` decimalv3(15, 2) NOT NULL,
  `c_mktsegment` varchar(10) NOT NULL,
  `c_comment` varchar(117) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `lineitem`
--

CREATE TABLE IF NOT EXISTS `lineitem` (
  `l_shipdate` date NOT NULL,
  `l_orderkey` bigint(20) NOT NULL,
  `l_linenumber` int(11) NOT NULL,
  `l_partkey` int(11) NOT NULL,
  `l_suppkey` int(11) NOT NULL,
  `l_quantity` decimalv3(15, 2) NOT NULL,
  `l_extendedprice` decimalv3(15, 2) NOT NULL,
  `l_discount` decimalv3(15, 2) NOT NULL,
  `l_tax` decimalv3(15, 2) NOT NULL,
  `l_returnflag` varchar(1) NOT NULL,
  `l_linestatus` varchar(1) NOT NULL,
  `l_commitdate` date NOT NULL,
  `l_receiptdate` date NOT NULL,
  `l_shipinstruct` varchar(25) NOT NULL,
  `l_shipmode` varchar(10) NOT NULL,
  `l_comment` varchar(44) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `nation`
--

CREATE TABLE IF NOT EXISTS `nation` (
  `n_nationkey` int(11) NOT NULL,
  `n_name` varchar(25) NOT NULL,
  `n_regionkey` int(11) NOT NULL,
  `n_comment` varchar(152) NULL
) ENGINE=OLAP
DUPLICATE KEY(`n_nationkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`n_nationkey`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `orders`
--

CREATE TABLE IF NOT EXISTS `orders` (
  `o_orderkey` bigint(20) NOT NULL,
  `o_orderdate` date NOT NULL,
  `o_custkey` int(11) NOT NULL,
  `o_orderstatus` varchar(1) NOT NULL,
  `o_totalprice` decimalv3(15, 2) NOT NULL,
  `o_orderpriority` varchar(15) NOT NULL,
  `o_clerk` varchar(15) NOT NULL,
  `o_shippriority` int(11) NOT NULL,
  `o_comment` varchar(79) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`o_orderkey`, `o_orderdate`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `part`
--

CREATE TABLE IF NOT EXISTS `part` (
  `p_partkey` int(11) NOT NULL,
  `p_name` varchar(55) NOT NULL,
  `p_mfgr` varchar(25) NOT NULL,
  `p_brand` varchar(10) NOT NULL,
  `p_type` varchar(25) NOT NULL,
  `p_size` int(11) NOT NULL,
  `p_container` varchar(10) NOT NULL,
  `p_retailprice` decimalv3(15, 2) NOT NULL,
  `p_comment` varchar(23) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `partsupp`
--

CREATE TABLE IF NOT EXISTS `partsupp` (
  `ps_partkey` int(11) NOT NULL,
  `ps_suppkey` int(11) NOT NULL,
  `ps_availqty` int(11) NOT NULL,
  `ps_supplycost` decimalv3(15, 2) NOT NULL,
  `ps_comment` varchar(199) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`ps_partkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `region`
--

CREATE TABLE IF NOT EXISTS `region` (
  `r_regionkey` int(11) NOT NULL,
  `r_name` varchar(25) NOT NULL,
  `r_comment` varchar(152) NULL
) ENGINE=OLAP
DUPLICATE KEY(`r_regionkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `supplier`
--

CREATE TABLE IF NOT EXISTS `supplier` (
  `s_suppkey` int(11) NOT NULL,
  `s_name` varchar(25) NOT NULL,
  `s_address` varchar(40) NOT NULL,
  `s_nationkey` int(11) NOT NULL,
  `s_phone` varchar(15) NOT NULL,
  `s_acctbal` decimalv3(15, 2) NOT NULL,
  `s_comment` varchar(101) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
"replication_num" = "1"
);

