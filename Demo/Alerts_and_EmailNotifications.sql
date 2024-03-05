//Alerts and Notifications
--https://docs.snowflake.com/en/guides-overview-alerts

//One-time stepup start
use role accountadmin;
--1.Creating a Notification Integration
CREATE NOTIFICATION INTEGRATION my_email_int
  TYPE=EMAIL
  ENABLED=TRUE
  ALLOWED_RECIPIENTS=('prakash.loganathaan@gmail.com');

show integrations;

--2.Granting the Privilege to Use the Notification Integration
GRANT USAGE ON INTEGRATION my_email_int TO ROLE sysadmin;

--3.Granting the Privileges to Create Alerts
GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin;
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE sysadmin;

--4.when you need the timestamps of the current schedule alert and the last alert that was successfully evaluated, use the following functions
GRANT DATABASE ROLE SNOWFLAKE.ALERT_VIEWER TO ROLE sysadmin;
//One-time stepup end

/*----------------------------------------------------------*/
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;

//Context Functions
--https://docs.snowflake.com/en/sql-reference/functions-context
SELECT CURRENT_CLIENT(), CURRENT_REGION(),  CURRENT_ACCOUNT_NAME(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();

//CREATE TASK
--CRON Options
//# __________ minute (0-59)
//# | ________ hour (0-23)
//# | | ______ day of month (1-31, or L)
//# | | | ____ month (1-12, JAN-DEC)
//# | | | | _ day of week (0-6, SUN-SAT, or L)
//# | | | | |
//# | | | | |
//  * * * * *
--https://docs.snowflake.com/en/sql-reference/sql/create-task
CREATE OR REPLACE TASK "DEMO_DB"."PUBLIC"."TEST_TASK"
WAREHOUSE = "COMPUTE_WH"
SCHEDULE = '1 MINUTE'
--SCHEDULE = 'USING CRON 15 21 * * 0 America/Chicago'
TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
--USER_TASK_TIMEOUT_MS = 18000000
COMMENT = 'Created by ploganathan'
AS
SELECT CURRENT_CLIENT(), CURRENT_REGION(),  CURRENT_ACCOUNT_NAME(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()
;

//Validation
show tasks like '%_TASK%'in schema DEMO_DB.PUBLIC;
--
ALTER TASK IF EXISTS "DEMO_DB"."PUBLIC"."TEST_TASK" RESUME;
ALTER TASK IF EXISTS "DEMO_DB"."PUBLIC"."TEST_TASK" SUSPEND;
--
//OPTIONAL: Manually Execute Task
EXECUTE TASK "DEMO_DB"."PUBLIC"."TEST_TASK";

//Check task execution status.
--https://docs.snowflake.com/en/sql-reference/functions/task_history
select *
from table(information_schema.task_history(scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),task_name=>'TEST_TASK'))
ORDER BY SCHEDULED_TIME DESC
;

select *
from table(information_schema.task_history(scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),task_name=>'TEST_TASK',error_only=>TRUE))
ORDER BY SCHEDULED_TIME DESC
;

/*--------------------------*/
--https://docs.snowflake.com/en/user-guide/alerts
--CRON Options
//# __________ minute (0-59)
//# | ________ hour (0-23)
//# | | ______ day of month (1-31, or L)
//# | | | ____ month (1-12, JAN-DEC)
//# | | | | _ day of week (0-6, SUN-SAT, or L)
//# | | | | |
//# | | | | |
//  * * * * *
CREATE OR REPLACE ALERT "DEMO_DB"."PUBLIC"."TEST_ALERT"
  WAREHOUSE = "COMPUTE_WH"
  SCHEDULE = '1 MINUTE'
  --SCHEDULE = 'USING CRON 15 21 * * 0 America/Chicago'
  --USER_TASK_TIMEOUT_MS = 18000000
  COMMENT = 'Created by ploganathan'
  IF (EXISTS (select count(QUERY_ID) as QUERY_ID_COUNT
              from table(information_schema.task_history(
              scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),
              task_name=>'TEST_TASK',
              error_only=>TRUE)
                        )

  ))
  THEN CALL SYSTEM$SEND_EMAIL( --Snowflake provided Stored Procedure
    'MY_EMAIL_INT',
    'prakash.loganathaan@gmail.com',
    'Email Alert: Test TASK Failed',
    'DEMO_DB.PUBLIC.TEST_TASK Failed for latest run. Please take a look'
);

//Suspending and Resuming an Alert
--ACCESS ISSUE: Cannot execute alert , EXECUTE ALERT privilege must be granted to owner role
ALTER ALERT "DEMO_DB"."PUBLIC"."TEST_ALERT" RESUME;
ALTER ALERT "DEMO_DB"."PUBLIC"."TEST_ALERT" SUSPEND;

//Viewing Details About an Alert
SHOW ALERTS IN SCHEMA "DEMO_DB"."PUBLIC";
DESC ALERT "DEMO_DB"."PUBLIC"."TEST_ALERT";
SHOW INTEGRATIONS;

//Monitoring the Execution of Alerts
SELECT 
*
FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(SCHEDULED_TIME_RANGE_START=>dateadd('hour',-24,current_timestamp())))
ORDER BY SCHEDULED_TIME DESC
;