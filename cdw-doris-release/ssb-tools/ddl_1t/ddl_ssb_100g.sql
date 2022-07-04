-- MySQL dump 10.14  Distrib 5.5.68-MariaDB, for Linux (x86_64)
--
-- Host: 127.0.0.1    Database: ssb_100g
-- ------------------------------------------------------
-- Server version	5.7.99

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `ssb_100g`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `ssb_100g`;

USE `ssb_100g`;

--
-- Table structure for table `customer`
--

DROP TABLE IF EXISTS `customer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dates`
--

DROP TABLE IF EXISTS `dates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `lineorder`
--

DROP TABLE IF EXISTS `lineorder`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `lineorder_flat`
--

DROP TABLE IF EXISTS `lineorder_flat`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `lineorder_flat` (
  `LO_ORDERDATE` int(11) NOT NULL,
  `LO_ORDERKEY` int(11) NOT NULL,
  `LO_LINENUMBER` tinyint(4) NOT NULL,
  `LO_CUSTKEY` int(11) NOT NULL,
  `LO_PARTKEY` int(11) NOT NULL,
  `LO_SUPPKEY` int(11) NOT NULL,
  `LO_ORDERPRIORITY` varchar(100) NOT NULL,
  `LO_SHIPPRIORITY` tinyint(4) NOT NULL,
  `LO_QUANTITY` tinyint(4) NOT NULL,
  `LO_EXTENDEDPRICE` int(11) NOT NULL,
  `LO_ORDTOTALPRICE` int(11) NOT NULL,
  `LO_DISCOUNT` tinyint(4) NOT NULL,
  `LO_REVENUE` int(11) NOT NULL,
  `LO_SUPPLYCOST` int(11) NOT NULL,
  `LO_TAX` tinyint(4) NOT NULL,
  `LO_COMMITDATE` date NOT NULL,
  `LO_SHIPMODE` varchar(100) NOT NULL,
  `C_NAME` varchar(100) NOT NULL,
  `C_ADDRESS` varchar(100) NOT NULL,
  `C_CITY` varchar(100) NOT NULL,
  `C_NATION` varchar(100) NOT NULL,
  `C_REGION` varchar(100) NOT NULL,
  `C_PHONE` varchar(100) NOT NULL,
  `C_MKTSEGMENT` varchar(100) NOT NULL,
  `S_NAME` varchar(100) NOT NULL,
  `S_ADDRESS` varchar(100) NOT NULL,
  `S_CITY` varchar(100) NOT NULL,
  `S_NATION` varchar(100) NOT NULL,
  `S_REGION` varchar(100) NOT NULL,
  `S_PHONE` varchar(100) NOT NULL,
  `P_NAME` varchar(100) NOT NULL,
  `P_MFGR` varchar(100) NOT NULL,
  `P_CATEGORY` varchar(100) NOT NULL,
  `P_BRAND` varchar(100) NOT NULL,
  `P_COLOR` varchar(100) NOT NULL,
  `P_TYPE` varchar(100) NOT NULL,
  `P_SIZE` tinyint(4) NOT NULL,
  `P_CONTAINER` varchar(100) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`LO_ORDERDATE`, `LO_ORDERKEY`)
COMMENT 'OLAP'
PARTITION BY RANGE(`LO_ORDERDATE`)
(PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
PARTITION p1993 VALUES [("19930101"), ("19940101")),
PARTITION p1994 VALUES [("19940101"), ("19950101")),
PARTITION p1995 VALUES [("19950101"), ("19960101")),
PARTITION p1996 VALUES [("19960101"), ("19970101")),
PARTITION p1997 VALUES [("19970101"), ("19980101")),
PARTITION p1998 VALUES [("19980101"), ("19990101")))
DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 48
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `part`
--

DROP TABLE IF EXISTS `part`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `supplier`
--

DROP TABLE IF EXISTS `supplier`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2023-08-24 14:37:12
