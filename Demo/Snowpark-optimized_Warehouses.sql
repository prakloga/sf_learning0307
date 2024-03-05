//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
//Snowpark-optimized Warehouses
--https://docs.snowflake.com/en/sql-reference/sql/create-warehouse
--https://docs.snowflake.com/en/user-guide/warehouses-snowpark-optimized

CREATE OR REPLACE WAREHOUSE snowpark_wh WITH
  WAREHOUSE_TYPE = 'STANDARD' --Newly introduced options
  WAREHOUSE_SIZE = 'XSMALL'
  MAX_CLUSTER_COUNT = 2
  MIN_CLUSTER_COUNT = 1
  SCALING_POLICY = STANDARD
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Snowpark NonOptimized Warehouse'
;

--Snowpark-optimized warehouses are recommended for workloads that have large memory requirements such as ML training use cases
CREATE OR REPLACE WAREHOUSE snowpark_opt_wh WITH
  WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED' --Newly introduced options
  WAREHOUSE_SIZE = 'MEDIUM'
  MAX_CLUSTER_COUNT = 2
  MIN_CLUSTER_COUNT = 1
  SCALING_POLICY = STANDARD
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Snowpark-optimized Warehouse'
;

//Validation
show warehouses;



