//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
//Create named external stage
create stage if not exists demo_db.public.sftutorials_ext_stg
URL = 's3://snowflake-docs'
;

--Validation
show stages in database demo_db;
--
list @sftutorials_ext_stg;
list '@sftutorials_ext_stg/tutorials/json/server/2.6/2016/07/15/15/';


//CREATE EXTERNAL TABLE
CREATE OR REPLACE EXTERNAL TABLE DEMO_DB.PUBLIC.SERVER_LOGS_ET
(device_type varchar as (VALUE:device_type::varchar)
,events array as (VALUE:events::array)
,filename varchar as METADATA$FILENAME 
,file_row_number number as METADATA$FILE_ROW_NUMBER
)
WITH LOCATION = @sftutorials_ext_stg/tutorials/json/server/
PATTERN = '.*/[0-9.]+.*/[0-9]+.*/[0-9]+.*/[0-9]+.*/[0-9]+.*' 
FILE_FORMAT = ( TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE)
REFRESH_ON_CREATE = TRUE
AUTO_REFRESH = TRUE
COMMENT = 'External Table'
;

//Optional
//Manually refresh the external table metadata 
ALTER EXTERNAL TABLE DEMO_DB.PUBLIC.SERVER_LOGS_ET REFRESH;
--
select * from DEMO_DB.PUBLIC.SERVER_LOGS_ET;
select * EXCLUDE VALUE from DEMO_DB.PUBLIC.SERVER_LOGS_ET;


