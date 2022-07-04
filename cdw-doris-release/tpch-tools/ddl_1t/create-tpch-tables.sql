--
-- Current Database: `tpch_1t`
--

CREATE DATABASE IF NOT EXISTS `tpch_1t`;

USE `tpch_1t`;

--
-- Table structure for table `customer`
--

CREATE TABLE IF NOT EXISTS `customer` (
  `C_CUSTKEY` bigint(20) NOT NULL,
  `C_NAME` varchar(25) NOT NULL,
  `C_ADDRESS` varchar(40) NOT NULL,
  `C_NATIONKEY` bigint(20) NOT NULL,
  `C_PHONE` char(15) NOT NULL,
  `C_ACCTBAL` DECIMAL(15, 2) NOT NULL,
  `C_MKTSEGMENT` char(10) NOT NULL,
  `C_COMMENT` varchar(117) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`C_CUSTKEY`, `C_NAME`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`C_CUSTKEY`) BUCKETS 24
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `lineitem`
--

CREATE TABLE IF NOT EXISTS `lineitem` (
  `L_ORDERKEY` bigint(20) NOT NULL,
  `L_PARTKEY` bigint(20) NOT NULL,
  `L_SUPPKEY` bigint(20) NOT NULL,
  `L_LINENUMBER` int(11) NOT NULL,
  `L_QUANTITY` DECIMAL(15, 2) NOT NULL,
  `L_EXTENDEDPRICE` DECIMAL(15, 2) NOT NULL,
  `L_DISCOUNT` DECIMAL(15, 2) NOT NULL,
  `L_TAX` DECIMAL(15, 2) NOT NULL,
  `L_RETURNFLAG` char(1) NOT NULL,
  `L_LINESTATUS` char(1) NOT NULL,
  `L_SHIPDATE` date NOT NULL,
  `L_COMMITDATE` date NOT NULL,
  `L_RECEIPTDATE` date NOT NULL,
  `L_SHIPINSTRUCT` char(25) NOT NULL,
  `L_SHIPMODE` char(10) NOT NULL,
  `L_COMMENT` varchar(44) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`L_ORDERKEY`, `L_PARTKEY`, `L_SUPPKEY`, `L_LINENUMBER`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 960
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "lineitem_orders_1t"
);;

--
-- Table structure for table `nation`
--

CREATE TABLE IF NOT EXISTS `nation` (
  `N_NATIONKEY` bigint(20) NOT NULL,
  `N_NAME` char(25) NOT NULL,
  `N_REGIONKEY` bigint(20) NOT NULL,
  `N_COMMENT` varchar(152) NULL
) ENGINE=OLAP
DUPLICATE KEY(`N_NATIONKEY`, `N_NAME`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `orders`
--

CREATE TABLE IF NOT EXISTS `orders` (
  `O_ORDERKEY` bigint(20) NOT NULL,
  `O_CUSTKEY` bigint(20) NOT NULL,
  `O_ORDERSTATUS` char(1) NOT NULL,
  `O_TOTALPRICE` DECIMAL(15, 2) NOT NULL,
  `O_ORDERDATE` date NOT NULL,
  `O_ORDERPRIORITY` char(15) NOT NULL,
  `O_CLERK` char(15) NOT NULL,
  `O_SHIPPRIORITY` int(11) NOT NULL,
  `O_COMMENT` varchar(79) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`O_ORDERKEY`, `O_CUSTKEY`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`O_ORDERKEY`) BUCKETS 960
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "lineitem_orders_1t"
);;

--
-- Table structure for table `part`
--

CREATE TABLE IF NOT EXISTS `part` (
  `P_PARTKEY` bigint(20) NOT NULL,
  `P_NAME` varchar(55) NOT NULL,
  `P_MFGR` char(25) NOT NULL,
  `P_BRAND` char(10) NOT NULL,
  `P_TYPE` varchar(25) NOT NULL,
  `P_SIZE` int(11) NOT NULL,
  `P_CONTAINER` char(10) NOT NULL,
  `P_RETAILPRICE` DECIMAL(15, 2) NOT NULL,
  `P_COMMENT` varchar(23) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`P_PARTKEY`, `P_NAME`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`P_PARTKEY`) BUCKETS 240
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "part_partsupp_1t"
);;

--
-- Table structure for table `partsupp`
--

CREATE TABLE IF NOT EXISTS `partsupp` (
  `PS_PARTKEY` bigint(20) NOT NULL,
  `PS_SUPPKEY` bigint(20) NOT NULL,
  `PS_AVAILQTY` int(11) NOT NULL,
  `PS_SUPPLYCOST` DECIMAL(15, 2) NOT NULL,
  `PS_COMMENT` varchar(199) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`PS_PARTKEY`, `PS_SUPPKEY`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`PS_PARTKEY`) BUCKETS 240
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "part_partsupp_1t"
);;

--
-- Table structure for table `region`
--

CREATE TABLE IF NOT EXISTS `region` (
  `R_REGIONKEY` bigint(20) NOT NULL,
  `R_NAME` char(25) NOT NULL,
  `R_COMMENT` varchar(152) NULL
) ENGINE=OLAP
DUPLICATE KEY(`R_REGIONKEY`, `R_NAME`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`R_REGIONKEY`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);;

--
-- Table structure for table `supplier`
--
CREATE TABLE IF NOT EXISTS `supplier` (
  `S_SUPPKEY` bigint(20) NOT NULL,
  `S_NAME` char(25) NOT NULL,
  `S_ADDRESS` varchar(40) NOT NULL,
  `S_NATIONKEY` bigint(20) NOT NULL,
  `S_PHONE` char(15) NOT NULL,
  `S_ACCTBAL` DECIMAL(15, 2) NOT NULL,
  `S_COMMENT` varchar(101) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`S_SUPPKEY`, `S_NAME`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`S_SUPPKEY`) BUCKETS 12
PROPERTIES (
"replication_num" = "1"
);;

