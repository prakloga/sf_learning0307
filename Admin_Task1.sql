CREATE DATABASE IF NOT EXISTS IDENTIFIER('"DEMO_DB"') COMMENT = 'Prakash Demo Database';
CREATE DATABASE IF NOT EXISTS IDENTIFIER('"UTIL_DB"') COMMENT = 'Prakash Demo Database';
CREATE DATABASE IF NOT EXISTS IDENTIFIER('"SNOWPARK_DB"') COMMENT = 'Prakash Snowpark Demo Database';

CREATE STAGE SNOWPARK_DB.PUBLIC.SF_INT_STG 
	DIRECTORY = ( ENABLE = true );

list @SNOWPARK_DB.PUBLIC.SF_INT_STG;

/*------------------------------------------------------------------------------------*/
//Create named external stage
/*Badge 1: Data Warehousing Workshop files
  We set up an S3 bucket on AWS in their US-West-2 Region. We named it "uni-lab-files." 
  Here is a link to the bucket: https://uni-lab-files.s3.us-west-2.amazonaws.com/
*/
create stage if not exists snowpark_db.public.sfdww_ext_stg
URL = 's3://uni-lab-files'
;

--Validation
show stages in database snowpark_db;
--
list @sfdww_ext_stg/;




create stage if not exists snowpark_db.public.sfdngw_ext_stg
URL = 's3://uni-kishore'
;

--Validation
show stages in database snowpark_db;
--
list @sfdngw_ext_stg/;




create stage if not exists snowpark_db.public.sfdlkw_ext_stg
URL = 's3://uni-klaus'
;

--Validation
show stages in database snowpark_db;
--
list @sfdlkw_ext_stg/;

/*------------------------------------------------------------------------------------*/
//Snowpark-optimized Warehouses
--https://docs.snowflake.com/en/sql-reference/sql/create-warehouse
--https://docs.snowflake.com/en/user-guide/warehouses-snowpark-optimized

--Snowpark-optimized warehouses are recommended for workloads that have large memory requirements such as ML training use cases
use role sysadmin;
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



