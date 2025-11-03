SHOW EXTERNAL LOCATIONS;

-- 1. Data to be loaded into Delta Table
LIST 'abfss://demo@dbxdl.dfs.core.windows.net/lineitems/' WITH (CREDENTIAL `dbxdl-storage-account-creds`);

SHOW CATALOGS;

DROP CATALOG IF EXISTS 03_CONCURRENCY CASCADE;
CREATE CATALOG IF NOT EXISTS 03_CONCURRENCY;

USE CATALOG 03_CONCURRENCY;

--CREATE SCHEMA IF NOT EXISTS BRONZE MANAGED LOCATION "abfss://delta@dbxdl.dfs.core.windows.net/default/";

USE SCHEMA bronze;

--DROP TABLE IF EXISTS lineitems;
SELECT * FROM PARQUET.`abfss://warehouse@dbxdl.dfs.core.windows.net/lineitems/*` LIMIT 5;

CREATE OR REPLACE TABLE 03_CONCURRENCY.default.RAW_LINEITEMS_WAREHOUSE (
  V VARIANT
) USING DELTA;

SELECT * FROM 03_CONCURRENCY.default.raw_lineitems_warehouse;

-- Copying the data over the table
-- !!! IMPORTANT !!!!
-- COPY INTO must list the source path every run to decide what’s new. Listing millions of files on ADLS Gen2 becomes slow and expensive (many list calls).
-- Auto Loader’s clean source options to DELETE/MOVE after ~30 days
COPY INTO 03_CONCURRENCY.default.RAW_LINEITEMS_WAREHOUSE
FROM (
  SELECT parse_json(to_json(struct(*))) AS V 
  FROM 'abfss://warehouse@dbxdl.dfs.core.windows.net/lineitems/*'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('singleVariantColumn' = 'true');

-- Counting 59,986,052
SELECT COUNT(*) FROM 03_CONCURRENCY.default.RAW_LINEITEMS_WAREHOUSE;
SELECT * FROM 03_CONCURRENCY.default.RAW_LINEITEMS_WAREHOUSE LIMIT 10;
-- Suspend SQL Warehouse
SELECT COUNT(*) FROM 03_CONCURRENCY.default.RAW_LINEITEMS_WAREHOUSE;

-- Selecting 5 random values
SELECT
  variant_get(V, '$.L_ORDERKEY') AS order_key,
  variant_get(V, '$.L_PARTKEY') AS part_key,
  variant_get(V, '$.L_SUPPKEY') AS supp_key,
  variant_get(V, '$.L_LINENUMBER') AS line_number,
  variant_get(V, '$.L_QUANTITY') AS quantity,
  variant_get(V, '$.L_EXTENDEDPRICE') AS extended_price,
  variant_get(V, '$.L_DISCOUNT') AS discount,
  variant_get(V, '$.L_TAX') AS tax,
  variant_get(V, '$.L_RETURNFLAG') AS return_flag,
  variant_get(V, '$.L_LINESTATUS') AS line_status,
  variant_get(V, '$.L_SHIPDATE') AS ship_date,
  variant_get(V, '$.L_COMMITDATE') AS commit_date,
  variant_get(V, '$.L_RECEIPTDATE') AS receipt_date,
  variant_get(V, '$.L_SHIPINSTRUCT') AS ship_instruct,
  variant_get(V, '$.L_SHIPMODE') AS ship_mode,
  variant_get(V, '$.L_COMMENT') AS comment
FROM 03_CONCURRENCY.default.RAW_LINEITEMS_WAREHOUSE
ORDER BY RAND() LIMIT 5;

-- QUERY RECORDS
SELECT
  variant_get(V, '$.L_ORDERKEY') AS order_key,
  variant_get(V, '$.L_PARTKEY') AS part_key,
  variant_get(V, '$.L_SUPPKEY') AS supp_key,
  variant_get(V, '$.L_LINENUMBER') AS line_number,
  variant_get(V, '$.L_QUANTITY') AS quantity,
  variant_get(V, '$.L_EXTENDEDPRICE') AS extended_price,
  variant_get(V, '$.L_DISCOUNT') AS discount,
  variant_get(V, '$.L_TAX') AS tax,
  variant_get(V, '$.L_RETURNFLAG') AS return_flag,
  variant_get(V, '$.L_LINESTATUS') AS line_status,
  variant_get(V, '$.L_SHIPDATE') AS ship_date,
  variant_get(V, '$.L_COMMITDATE') AS commit_date,
  variant_get(V, '$.L_RECEIPTDATE') AS receipt_date,
  variant_get(V, '$.L_SHIPINSTRUCT') AS ship_instruct,
  variant_get(V, '$.L_SHIPMODE') AS ship_mode,
  variant_get(V, '$.L_COMMENT') AS comment
FROM 03_CONCURRENCY.default.RAW_LINEITEMS_WAREHOUSE
-- This will failed due to the premature SQL engine, the attribute needs to be casted twice
--WHERE order_key IN (
WHERE CAST(variant_get(V, '$.L_ORDERKEY') AS STRING) IN (
'30370724',
'43675749',
'46386755',
'39896960',
'51780611'
);

-- Creating table
CREATE OR REPLACE TABLE 03_CONCURRENCY.default.LINEITEMS_WAREHOUSE
USING DELTA
AS
SELECT
  CAST(variant_get(V, '$.L_ORDERKEY') AS VARCHAR(15)) AS L_ORDERKEY,
  CAST(variant_get(V, '$.L_PARTKEY') AS VARCHAR(15)) AS L_PARTKEY,
  CAST(variant_get(V, '$.L_SUPPKEY') AS VARCHAR(15)) AS L_SUPPKEY,
  CAST(variant_get(V, '$.L_LINENUMBER') AS INT) AS L_LINENUMBER,
  CAST(variant_get(V, '$.L_QUANTITY') AS FLOAT) AS L_QUANTITY,
  CAST(variant_get(V, '$.L_EXTENDEDPRICE') AS FLOAT) AS L_EXTENDEDPRICE,
  CAST(variant_get(V, '$.L_DISCOUNT') AS FLOAT) AS L_DISCOUNT,
  CAST(variant_get(V, '$.L_TAX') AS FLOAT) AS L_TAX,
  CAST(variant_get(V, '$.L_RETURNFLAG') AS VARCHAR(30)) AS L_RETURNFLAG,
  CAST(variant_get(V, '$.L_LINESTATUS') AS VARCHAR(30)) AS L_LINESTATUS,
  CAST(variant_get(V, '$.L_SHIPDATE') AS DATE) AS L_SHIPDATE,
  CAST(variant_get(V, '$.L_COMMITDATE') AS DATE) AS L_COMMITDATE,
  CAST(variant_get(V, '$.L_RECEIPTDATE') AS DATE) AS L_RECEIPTDATE,
  CAST(variant_get(V, '$.L_SHIPINSTRUCT') AS VARCHAR(30)) AS L_SHIPINSTRUCT,
  CAST(variant_get(V, '$.L_SHIPMODE') AS VARCHAR(30)) AS L_SHIPMODE,
  CAST(variant_get(V, '$.L_COMMENT') AS VARCHAR(100)) AS L_COMMENT
FROM 03_CONCURRENCY.default.raw_lineitems_warehouse;

SELECT * FROM 03_CONCURRENCY.default.lineitems_warehouse LIMIT 10;

-- CLEAN UP
-- USE CATALOG 03_CONCURRENCY;
-- USE SCHEMA default;

-- DROP TABLE IF EXISTS 03_CONCURRENCY.default.lineitems;
-- DROP TABLE IF EXISTS 03_CONCURRENCY.default.lineitems_warehouse;
-- DROP TABLE IF EXISTS 03_CONCURRENCY.default.raw_lineitems;
-- DROP TABLE IF EXISTS 03_CONCURRENCY.default.raw_lineitems_warehouse;
-- DROP TABLE IF EXISTS 03_CONCURRENCY.default.raw_lineitems_dlt;

-- DROP SCHEMA IF EXISTS 03_CONCURRENCY.default CASCADE;
-- DROP SCHEMA IF EXISTS 03_CONCURRENCY.silver CASCADE;
-- DROP SCHEMA IF EXISTS 03_CONCURRENCY.default CASCADE;