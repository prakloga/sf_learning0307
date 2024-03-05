/*
🥋 Exploring the Source of the Databases

📓  When we navigate to to the Shared Data page, we gain some insight into the source of our databases.
Notice that the database called SNOWFLAKE_SAMPLE_DATA is coming from an account called SFSALESSHARED (followed by a schema that will vary by region).
This account named their outbound share "SAMPLE_DATA." 
It is only in our account that this data appears under the name SNOWFLAKE_SAMPLE_DATA.
*/
use role accountadmin;
//🥋 Create a SQL Worksheet & Name It "CMCW Lesson 2"
alter database that_really_cool_sample_stuff rename to snowflake_sample_data;

//🎯 Challenge Lab: What Can You Do to the SNOWFLAKE Database?
//🥋 What Databases Can You See as SYSADMIN?
//🥋 Grant Privileges to the Share for the SYSADMIN Role?
//📓 Granting The Same Rights - But Using Code
--If you wanted to use code to carry out the task above, this is what it would look like:
GRANT IMPORTED PRIVILEGES ON DATABASE IDENTIFIER('"SNOWFLAKE_SAMPLE_DATA"') TO ROLE IDENTIFIER('"SYSADMIN"');
/*
Why do you think the privileges are called IMPORTED PRIVILEGES in the code we just ran?
Because privileges for a shared database are pre-defined for maximum data security.
*/


//🥋 Use Select Statements to Look at Sample Data
use role sysadmin;

--Check range of values in the Market Segment Column
select distinct C_MKTSEGMENT from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segment have the most customers
select C_MKTSEGMENT, count(*) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER group by C_MKTSEGMENT order by count(*);

//🥋 Join and Aggregate Shared Data
--Nations table
select N_NATIONKEY, N_NAME, N_REGIONKEY from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

--Regions table
select R_REGIONKEY, R_NAME from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

--Join the table and sort
select
 r.R_NAME as region
,n.N_NAME as nation
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION as n
join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION as r
on n.N_REGIONKEY = r.R_REGIONKEY
order by r.R_NAME, n.N_NAME asc
;

--Group and count rows per region
select
 r.R_NAME as region
,count(n.N_NAME) as num_countries
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION as n
join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION as r
on n.N_REGIONKEY = r.R_REGIONKEY
group by r.R_NAME
;

//🥋 Export Native and Shared Data
/*
The real value of consuming shared data is:
1.Someone else will maintain it over time and keep it fresh
2.Someone else will pay to store it
3.You will only pay to query it
*/
//🥋 Set Your Default Role to SYSADMIN
select current_user();
ALTER USER IDENTIFIER('"PLOGANATHAN"') set DEFAULT_ROLE = 'SYSADMIN';

//🎯 Give SYSADMIN Access to COMPUTE_WH
GRANT OWNERSHIP ON WAREHOUSE IDENTIFIER('"COMPUTE_WH"') TO ROLE IDENTIFIER('"SYSADMIN"');

//🎯 Set Your User Profile Default Warehouse

//🥋 Create a Local Database Named UTIL_DB
CREATE DATABASE IF NOT EXISTS IDENTIFIER('"UTIL_DB"') COMMENT = '';
GRANT OWNERSHIP ON DATABASE IDENTIFIER('"UTIL_DB"') TO ROLE IDENTIFIER('"SYSADMIN"');
GRANT OWNERSHIP ON SCHEMA IDENTIFIER('"UTIL_DB"."PUBLIC"') TO ROLE IDENTIFIER('"SYSADMIN"');






