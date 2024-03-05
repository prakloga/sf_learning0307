//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
//Query Acceleration Service (QAS)
--https://docs.snowflake.com/en/user-guide/query-acceleration-service
--https://docs.snowflake.com/en/user-guide/tutorials/query-acceleration-service#introduction
/*
The query acceleration service can accelerate parts of the query workload in a warehouse. 
When it is enabled for a warehouse, it can improve overall warehouse performance by reducing the impact of outlier queries, which are queries that use more resources than the typical query. 

1.Ad hoc analytics.
2.Workloads with unpredictable data volume per query.
3.Queries with large scans and selective filters.
*/

--Create two virtual warehouse with/without QAS
--https://docs.snowflake.com/en/sql-reference/sql/create-warehouse
--Without Query Acceleration 
CREATE OR REPLACE WAREHOUSE noqas_wh WITH
  WAREHOUSE_TYPE = 'STANDARD' --New option
  WAREHOUSE_SIZE = 'MEDIUM'
  ENABLE_QUERY_ACCELERATION = FALSE --New option
  INITIALLY_SUSPENDED = TRUE
  AUTO_SUSPEND = 60
  COMMENT = 'query acceleration service is not enabled'
  ;
  

--Without Query Acceleration
CREATE OR REPLACE WAREHOUSE qas_wh WITH
  WAREHOUSE_TYPE = 'STANDARD' --New option
  WAREHOUSE_SIZE = 'MEDIUM'
  ENABLE_QUERY_ACCELERATION = TRUE --New option
  QUERY_ACCELERATION_MAX_SCALE_FACTOR = 3 --Default is 8 | New option
  INITIALLY_SUSPENDED = TRUE
  AUTO_SUSPEND = 60
  COMMENT = 'query acceleration service is enabled'
  ;

/*-----------------*/
use warehouse noqas_wh;

--eligible query acceleration
SELECT d.d_year as "Year",
       i.i_brand_id as "Brand ID",
       i.i_brand as "Brand",
       SUM(ss_net_profit) as "Profit"
FROM   snowflake_sample_data.tpcds_sf10tcl.date_dim    d,
       snowflake_sample_data.tpcds_sf10tcl.store_sales s,
       snowflake_sample_data.tpcds_sf10tcl.item        i
WHERE  d.d_date_sk = s.ss_sold_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND i.i_manufact_id = 939
  AND d.d_moy = 12
GROUP BY d.d_year,
         i.i_brand,
         i.i_brand_id
ORDER BY 1, 4, 2
LIMIT 200;

--QueryID: 01b0ae74-0504-d945-0002-02db0002709e | 2m 19s

--To identify the queries that might benefit from the query acceleration service, you can use the SYSTEM$ESTIMATE_QUERY_ACCELERATION function
--https://docs.snowflake.com/en/sql-reference/functions-system
SELECT PARSE_JSON(SYSTEM$ESTIMATE_QUERY_ACCELERATION('01b0ae74-0504-d945-0002-02db0002709e'));
/*
{
  "estimatedQueryTimes": {
    "1": 77,
    "2": 54,
    "3": 43
  },
  "originalQueryTime": 138.833,
  "queryUUID": "01b0ae74-0504-d945-0002-02db0002709e",
  "status": "eligible",
  "upperLimitScaleFactor": 3
}
*/


/*--------------------*/
use warehouse qas_wh;

SELECT d.d_year as "Year",
       i.i_brand_id as "Brand ID",
       i.i_brand as "Brand",
       SUM(ss_net_profit) as "Profit"
FROM   snowflake_sample_data.tpcds_sf10tcl.date_dim    d,
       snowflake_sample_data.tpcds_sf10tcl.store_sales s,
       snowflake_sample_data.tpcds_sf10tcl.item        i
WHERE  d.d_date_sk = s.ss_sold_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND i.i_manufact_id = 939
  AND d.d_moy = 12
GROUP BY d.d_year,
         i.i_brand,
         i.i_brand_id
ORDER BY 1, 4, 2
LIMIT 300;

--QueryID: 01b0ae79-0504-d946-0002-02db000290ce | 36s
SELECT PARSE_JSON(SYSTEM$ESTIMATE_QUERY_ACCELERATION('01b0ae79-0504-d946-0002-02db000290ce'));
/*
{
  "estimatedQueryTimes": {},
  "originalQueryTime": 39.842,
  "queryUUID": "01b09f25-0504-d7da-0002-02db0001238e",
  "status": "accelerated",
  "upperLimitScaleFactor": 0
}
*/

--Identify the queries that might benefit the most from the service by the amount of query execution time that is eligible for acceleration:
SELECT 
 query_id
,eligible_query_acceleration_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
ORDER BY eligible_query_acceleration_time DESC
;

