//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
//RESULT_SCAN
--https://docs.snowflake.com/en/sql-reference/functions/result_scan

//LAST_QUERY_ID: Positive numbers start with the first query executed in the session | Negative numbers start with the most recently-executed query in the session
select last_query_id(-1);

//The output returns table metadata and properties
show tables in schema demo_db.public;
show tables in database demo_db;
show warehouses;
show stages in schema demo_db.public;

//Persisted Query Results
create or replace table demo_db.public.table_metadata as select * from table(result_scan(last_query_id()));

//validate the target table
select * from demo_db.public.table_metadata;

