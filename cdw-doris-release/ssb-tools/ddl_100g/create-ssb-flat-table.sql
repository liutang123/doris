--
-- Current Database: `ssb_100g`
--

CREATE DATABASE IF NOT EXISTS `ssb_100g`;

USE `ssb_100g`;

--
-- Table structure for table `lineorder_flat`
--
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

