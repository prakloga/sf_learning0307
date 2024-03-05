//Lesson 5: Productionizing Our Work  🥋 Productionizing the Load 
use role sysadmin;
use warehouse compute_wh;
use schema AGS_GAME_AUDIENCE.RAW;
/*----------------------------------------------------------------------*/
/*
📓 What About Next Time?
Kishore has now successfully taken data from a file (extracted it), enhanced it (transformed it) and put it into a database table (loaded it). 

Along the way, he:

normalized the data from a JSON format into a relational presentation,
added the local time zone using IP address information 
calculated a timestamp in each gamer's local time zone.
added columns that can be used to group gaming events by day of week and/or time of day. 
The problem is, he did this just one time, for one file.

What if Agnie wants him to pull in a new log file, every day? That could be a lot of work!

How can Kishore automate the movement of the data all the way from the external file through to loading of the enhanced table? Generically, this can be referred to as "production-izing" the data load. 

There are a number of ways to productionize this load process, but we'll start by learning about tasks!
*/
//🥋 Create a Simple Task
--https://docs.snowflake.com/en/sql-reference/sql/create-task?utm_source=snowscope&utm_medium=serp&utm_term=create+task
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '5 MINUTE'
AS
select 'hello'
;

//🥋 SYSADMIN Privileges for Executing Tasks
--you have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task
use role accountadmin;
grant execute task on account to role sysadmin;

--Now you should be able to run the task, even if your role is set to sysadmine
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

/*
📓 Running the Task
Once we have a task, we would have to turn it "on" to start the 5 minute clock. We don't want to do that, yet, so we'll execute the task, manually. 
worksheetsWe can manually run the task using: 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;
*/
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

//📓 Checking Task History
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),
    task_name=>'LOAD_LOGS_ENHANCED'));

//🥋 Execute the Task a Few More Times
--Run the task a few times to see changes in the RUN HISTORY
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

//📓 Making the Task Better
//🎯 Use the CTAS Logic in the Task
//📓  A Fancy, Important, Serious Task
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '5 MINUTE'
AS
create or replace table ags_game_audience.enhanced.logs_enhanced 
as
select
 logs.ip_address
,logs.user_login as GAMER_NAME
,logs.user_event as GAME_EVENT_NAME
,logs.datetime_iso8601 as GAME_EVENT_UTC
,loc.city
,loc.region
,loc.country
,loc.timezone as GAMER_LTZ_NAME
,CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601) as GAME_EVENT_LTZ
,dayname(GAME_EVENT_LTZ) as DOW_NAME
,tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
on IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) between start_ip_int and end_ip_int
join AGS_GAME_AUDIENCE.RAW.time_of_day_lu as tod 
on hour(GAME_EVENT_LTZ) = tod.hour
;

//🥋 Executing the Task to Load More Rows
--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; --146

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; --146

/*
🎯 Convert Your Task so It Inserts Rows
1) Add this line of code just above the SELECT line in your task:
INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
2) Run the CREATE OR REPLACE TASK command. This will replace the old task with this new version. 
3) EXECUTE the task manually. (Run it a few times if you want!)
4) Check the number of rows in the table (has it changed now?)
5) Check the RUN HISTORY to make sure the task is still running without errors. 
*/
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '5 MINUTE'
AS
insert into ags_game_audience.enhanced.logs_enhanced 
select
 logs.ip_address
,logs.user_login as GAMER_NAME
,logs.user_event as GAME_EVENT_NAME
,logs.datetime_iso8601 as GAME_EVENT_UTC
,loc.city
,loc.region
,loc.country
,loc.timezone as GAMER_LTZ_NAME
,CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601) as GAME_EVENT_LTZ
,dayname(GAME_EVENT_LTZ) as DOW_NAME
,tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
on IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) between start_ip_int and end_ip_int
join AGS_GAME_AUDIENCE.RAW.time_of_day_lu as tod 
on hour(GAME_EVENT_LTZ) = tod.hour
;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; --146

//📓 Checking Task Run History
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),
    task_name=>'LOAD_LOGS_ENHANCED'));

//🥋 Trunc & Reload Like It's Y2K!
--first we dump all the rows out of the table
truncate table ags_game_audience.enhanced.logs_enhanced;

--then we put them all back in
insert into ags_game_audience.enhanced.logs_enhanced 
select
 logs.ip_address
,logs.user_login as GAMER_NAME
,logs.user_event as GAME_EVENT_NAME
,logs.datetime_iso8601 as GAME_EVENT_UTC
,loc.city
,loc.region
,loc.country
,loc.timezone as GAMER_LTZ_NAME
,CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601) as GAME_EVENT_LTZ
,dayname(GAME_EVENT_LTZ) as DOW_NAME
,tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
on IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) between start_ip_int and end_ip_int
join AGS_GAME_AUDIENCE.RAW.time_of_day_lu as tod 
on hour(GAME_EVENT_LTZ) = tod.hour
;

--we should do this every 5 minutes from now until the next millenium - Y3K!!!
//🥋 Create a Backup Copy of the Table
--clone the table to save this version as a backup
--since it holds the records from the UPDATED FEED file, we.ill name it _UP
create table ags_game_audience.enhanced.logs_enhanced_up
clone ags_game_audience.enhanced.logs_enhanced;

show tables like 'logs_enhanced%' in account;

//📓 Sophisticated 2010's - The Merge!
//🥋 Truncate Again for a Fresh Start
--let's truncate so we can start the load over again
-- remember we have that cloned back up so it's fine
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

//🥋 Build Your Insert Merge
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '5 MINUTE'
AS
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED as e
USING (select
 logs.ip_address
,logs.user_login as GAMER_NAME
,logs.user_event as GAME_EVENT_NAME
,logs.datetime_iso8601 as GAME_EVENT_UTC
,loc.city
,loc.region
,loc.country
,loc.timezone as GAMER_LTZ_NAME
,CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601) as GAME_EVENT_LTZ
,dayname(GAME_EVENT_LTZ) as DOW_NAME
,tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
on IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) between start_ip_int and end_ip_int
join AGS_GAME_AUDIENCE.RAW.time_of_day_lu as tod 
on hour(GAME_EVENT_LTZ) = tod.hour
) as r
ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
and r.GAME_EVENT_NAME = e.GAME_EVENT_NAME
--
WHEN NOT MATCHED THEN
INSERT(ip_address,GAMER_NAME,GAME_EVENT_NAME,GAME_EVENT_UTC,city,region,country,GAMER_LTZ_NAME,GAME_EVENT_LTZ,DOW_NAME,tod_name)
VALUES(r.ip_address,r.GAMER_NAME,r.GAME_EVENT_NAME,r.GAME_EVENT_UTC,r.city,r.region,r.country,r.GAMER_LTZ_NAME,r.GAME_EVENT_LTZ,r.DOW_NAME,r.tod_name)
;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; --146
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

//📓 Checking Task Run History
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),
    task_name=>'LOAD_LOGS_ENHANCED'));

/*
After creating the task, execute it to check that it succeeds. Like this: EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;
What happens if you run it more than once? Does it create multiple copies of each record? Or is the process IDEMPOTENT? 
*/

//🥋 Testing Cycle (Optional)
--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; --146

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; --147

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 














