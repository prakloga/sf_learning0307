CREATE DATABASE IF NOT EXISTS IDENTIFIER('"UTIL_DB"') COMMENT = '';
GRANT OWNERSHIP ON DATABASE IDENTIFIER('"UTIL_DB"') TO ROLE IDENTIFIER('"SYSADMIN"');
GRANT OWNERSHIP ON SCHEMA IDENTIFIER('"UTIL_DB"."PUBLIC"') TO ROLE IDENTIFIER('"SYSADMIN"');

select current_account(); --EBA62826
//DORA Auto-Grader
//https://learn.snowflake.com/news
//Is the GRADER Function working?
use role accountadmin;
use database util_db; 
use schema public; 

//Create an API Integration
create or replace api integration dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

//Create the GRADER Function
create or replace external function util_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp,current_account, current_statement) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'
; 

show integrations;

//🎯 Give the SYSADMIN Role Access to the Grader Function
GRANT USAGE ON FUNCTION UTIL_DB.PUBLIC.GRADER(VARCHAR, BOOLEAN, NUMBER, NUMBER, VARCHAR) TO ROLE IDENTIFIER('"SYSADMIN"');

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW10' as step
 ,( select count(*)
    from snowflake.account_usage.databases
    where (database_name in ('WEATHERSOURCE','INTERNATIONAL_CURRENCIES')
           and type = 'IMPORTED DATABASE'
           and deleted is null)
    or (database_name = 'MARKETING'
          and type = 'STANDARD'
          and deleted is null)
   ) as actual
 , 3 as expected
 ,'ACME Account Set up nicely' as description
); 

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW11' as step
 ,( select count(*) 
   from MARKETING.MAILERS.DETROIT_ZIPS) as actual
 , 9 as expected
 ,'Detroit Zips' as description
); 

--RUN THIS DORA CHECK IN YOUR ACME ACCOUNT
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW14' as step
 ,( select count(*) 
   from STOCK.UNSOLD.LOTSTOCK
   where engine like '%.5 L%'
   or plant_name like '%z, Sty%'
   or desc2 like '%xDr%') as actual
 , 145 as expected
 ,'Intentionally cryptic test' as description
); 



