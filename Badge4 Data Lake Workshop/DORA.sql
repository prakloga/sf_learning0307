//🤖 Can You Find the Function Using Code? 
-- where did you put the function?
show user functions in account;
select current_account(); --LWB18040

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

//🤖 Is DORA Working? Run This to Find Out!
select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 
--Part2--

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
/*----------------------------------------------------------------------*/
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW01' as step
  ,( select count(*)
      from ZENAS_ATHLEISURE_DB.INFORMATION_SCHEMA.STAGES
      where stage_url ilike ('%/clothing%')
      or stage_url ilike ('%/zenas_metadata%')
      or stage_url like ('%/sneakers%')
   ) as actual
, 3 as expected
, 'Stages for Klaus bucket look good' as description
); 

/*
Only mark this page complete if you saw a green check ✅ in the results section of your Snowflake Account. 
If you have any problems, watch this 8-minute video: https://youtu.be/HS3FcrcwSTw
*/

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
   'DLKW02' as step
   ,( select sum(tally) from
        (select count(*) as tally
        from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_PRODUCT_LINE
        where length(product_code) > 7 
        union
        select count(*) as tally
        from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES
        where LEFT(sizes_available,2) = char(13)||char(10))     
     ) as actual
   ,0 as expected
   ,'Leave data where it lands.' as description
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW03' as step
 ,( select count(*) from ZENAS_ATHLEISURE_DB.PRODUCTS.CATALOG) as actual
 ,198 as expected
 ,'Cross-joined view exists' as description
); 


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW04' as step
 ,( select count(*) 
  from zenas_athleisure_db.products.catalog_for_website 
  where upsell_product_desc like '%NUS:%') as actual
 ,6 as expected
 ,'Relentlessly resourceful' as description
); 


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW05' as step
 ,( select sum(tally)
   from
     (select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.stages 
      union all
      select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.file_formats)) as actual
 ,4 as expected
 ,'Camila\'s Trail Data is Ready to Query' as description
 ); 

 
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW06' as step
 ,( select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.views 
      where table_name in ('CHERRY_CREEK_TRAIL','DENVER_AREA_TRAILS')) as actual
 ,2 as expected
 ,'Mel\'s views on the geospatial data from Camila' as description
 ); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
  'DLKW07' as step
   ,( select round(max(max_northsouth))
      from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES)
      as actual
 ,40 as expected
 ,'Trails Northern Extent' as description
 ); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
  'DLKW08' as step
  ,( select truncate(distance_to_melanies)
      from mels_smoothie_challenge_db.locations.denver_bike_shops
      where name like '%Mojo%') as actual
  ,14084 as expected
  ,'Bike Shop View Distance Calc works' as description
 ); 

 
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
  'DLKW09' as step
  ,( select row_count
     from mels_smoothie_challenge_db.information_schema.tables
     where table_schema = 'TRAILS'
    and table_name = 'SMV_CHERRY_CREEK_TRAIL')   
   as actual
  ,3526 as expected
  ,'Secure Materialized View Created' as description
 ); 






