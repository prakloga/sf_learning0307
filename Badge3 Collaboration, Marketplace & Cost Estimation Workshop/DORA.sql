//🤖 Can You Find the Function Using Code? 
-- where did you put the function?
show user functions in account;

-- did you put it here?
select * 
from util_db.information_schema.functions
where function_name = 'GRADER'
and function_catalog = 'UTIL_DB'
and function_owner = 'ACCOUNTADMIN';

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

//🥋 Is DORA Working? Run This to Find Out!
USE ROLE SYSADMIN;

select GRADER(step,(actual = expected), actual, expected, description) as graded_results from (
SELECT 'DORA_IS_WORKING' as step
 ,(select 223 ) as actual
 ,223 as expected
 ,'Dora is working!' as description
); 


//🤖 Run This Code to Create a Second DORA External Function!
use role accountadmin;
--to create the function your role should be set to ACCOUNTADMIN
--once you create it, you can then set it to SYSADMIN
create or replace external function util_db.public.greeting(
      email varchar
    , firstname varchar
    , middlename varchar
    , lastname varchar)
returns variant
api_integration = dora_api_integration
context_headers = (current_timestamp,current_account, current_statement) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/greeting'
;

//🤖 EDIT (and Run) this CODE to Greet DORA!!
-- PLEASE EDIT THIS TO PUT YOUR EMAIL, FIRST, MIDDLE, & LAST NAMES (use single quotes around them)
--select util_db.public.greeting(<email>, <first name>, <middle name>, <last name>);
--
select util_db.public.greeting('prakash.loganathan@att.com', 'Prakash', '', 'Loganathan');
/*
[   {     "ACCOUNT_LOCATOR": "LWB18040",     "EMAIL": "prakash.loganathan@att.com",     "FIRSTNAME": "Prakash",     "LASTNAME": "Loganathan",     "MIDDLENAME": ""   } ]
*/

//🥋 Visit Snowflake's Pricing Page to Look Up the Cost Per Credit for Your Account
--Navigate to: https://www.snowflake.com/en/data-cloud/pricing-options/

//🥋 Navigate to Your Cost Management Page

/*----------------------------------------------------------------------*/
-- set your worksheet drop lists or write and run USE commands
-- YOU WILL NEED TO USE ACCOUNTADMIN ROLE on this test.

--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from( 
 SELECT 'CMCW01' as step
 ,( select count(*) 
   from snowflake.account_usage.databases
   where database_name = 'INTL_DB' 
   and deleted is null) as actual
 , 1 as expected
 ,'Created INTL_DB' as description
 );

 
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW02' as step
 ,( select count(*) 
   from INTL_DB.INFORMATION_SCHEMA.TABLES 
   where table_schema = 'PUBLIC' 
   and table_name = 'INT_STDS_ORG_3166') as actual
 , 1 as expected
 ,'ISO table created' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from( 
SELECT 'CMCW03' as step 
 ,(select row_count 
   from INTL_DB.INFORMATION_SCHEMA.TABLES  
   where table_name = 'INT_STDS_ORG_3166') as actual 
 , 249 as expected 
 ,'ISO Table Loaded' as description 
); 

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW04' as step
 ,( select count(*) 
   from INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO) as actual
 , 249 as expected
 ,'Nations Sample Plus Iso' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW05' as step
 ,(select row_count 
  from INTL_DB.INFORMATION_SCHEMA.TABLES 
  where table_schema = 'PUBLIC' 
  and table_name = 'COUNTRY_CODE_TO_CURRENCY_CODE') as actual
 , 265 as expected
 ,'CCTCC Table Loaded' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW06' as step
 ,(select row_count 
  from INTL_DB.INFORMATION_SCHEMA.TABLES 
  where table_schema = 'PUBLIC' 
  and table_name = 'CURRENCIES') as actual
 , 151 as expected
 ,'Currencies table loaded' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
 SELECT 'CMCW07' as step 
,( select count(*) 
  from INTL_DB.PUBLIC.SIMPLE_CURRENCY ) as actual
, 265 as expected
,'Simple Currency Looks Good' as description
);

--This DORA Check Requires that you RUN two Statements, one right after the other
show shares in account;

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the SHOW command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW08' as step
 ,( select IFF(count(*)>0,1,0) 
    from table(result_scan(last_query_id())) 
    where "kind" = 'OUTBOUND'
    and "database_name" = 'INTL_DB') as actual
 , 1 as expected
 ,'Outbound Share Created From INTL_DB' as description
); 

--This DORA Check Requires that you RUN two Statements, one right after the other
show resource monitors in account;

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the SHOW command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW09' as step
 ,( select IFF(count(*)>0,1,0) 
    from table(result_scan(last_query_id())) 
    where "name" = 'DAILY_3_CREDIT_LIMIT'
    and "credit_quota" = 3
    and "frequency" = 'DAILY') as actual
 , 1 as expected
 ,'Resource Monitors Exist' as description
); 

//Run below 2 statements in ACME snowflake account
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

--RUN THIS DORA CHECK IN YOUR ORIGINAL TRIAL ACCOUNT
select grader(step, (actual = expected), actual, expected, description) as graded_results 
from ( SELECT 'CMCW12' as step ,( select count(*) from SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS where account_name = 'ACME' 
and region like 'AZURE_%' and deleted_on is null) as actual , 1 as expected ,'ACME Account Added on Azure Platform' as description ); 

--RUN THIS DORA CHECK IN YOUR ORIGINAL TRIAL ACCOUNT
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW13' as step
 ,( select count(*) 
   from SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS 
   where account_name = 'AUTO_DATA_UNLIMITED' 
   and region like 'GCP_%'
   and deleted_on is null) as actual
 , 1 as expected
 ,'ADU Account Added on GCP' as description
);




