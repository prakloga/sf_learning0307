CREATE DATABASE IF NOT EXISTS IDENTIFIER('"DEMO_DB"') COMMENT = 'Prakash Demo Database';
GRANT OWNERSHIP ON DATABASE IDENTIFIER('"DEMO_DB"') TO ROLE IDENTIFIER('"SYSADMIN"');
GRANT OWNERSHIP ON SCHEMA IDENTIFIER('"DEMO_DB"."PUBLIC"') TO ROLE IDENTIFIER('"SYSADMIN"');
Create or replace SCHEMA IDENTIFIER('"DEMO_DB"."FAKE_DATA"');
Create or replace SCHEMA IDENTIFIER('"DEMO_DB"."WINDOW_FUNC"');

CREATE DATABASE IF NOT EXISTS IDENTIFIER('"UTIL_DB"') COMMENT = 'Prakash Demo Database';
GRANT OWNERSHIP ON DATABASE IDENTIFIER('"UTIL_DB"') TO ROLE IDENTIFIER('"SYSADMIN"');
GRANT OWNERSHIP ON SCHEMA IDENTIFIER('"UTIL_DB"."PUBLIC"') TO ROLE IDENTIFIER('"SYSADMIN"');

CREATE DATABASE IF NOT EXISTS IDENTIFIER('"SNOWPARK_DB"') COMMENT = 'Prakash Snowpark Demo Database';

CREATE STAGE SNOWPARK_DB.PUBLIC.SF_INT_STG 
	DIRECTORY = ( ENABLE = true );

list @SNOWPARK_DB.PUBLIC.SF_INT_STG;

CREATE STAGE SNOWPARK_DB.PUBLIC.SF_UDF_INT_STG 
	DIRECTORY = ( ENABLE = true );

list @SNOWPARK_DB.PUBLIC.SF_UDF_INT_STG;
//https://docs.snowflake.com/en/sql-reference/functions-context

--Account Identifiers
//https://docs.snowflake.com/en/user-guide/admin-account-identifier
select current_account(), current_region();
--https://lwb18040.us-east-1.snowflakecomputing.com
--TJYADXZ.ENB30427

//https://docs.snowflake.com/en/sql-reference/functions/system_allowlist
select SYSTEM$ALLOWLIST();

//Flatten JSON input into table form
select
 VALUE:host::string as host
,VALUE:port as port
,VALUE:type::string as type
from table(flatten(input => parse_json(SYSTEM$ALLOWLIST())))
;


select * from snowflake.account_usage.sessions order by created_on desc limit 100;