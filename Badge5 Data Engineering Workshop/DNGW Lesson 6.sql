//Lesson 6: Productionizing Across the Pipeline  📓 We Have a Pipeline! 
use role sysadmin;
use warehouse compute_wh;
use schema AGS_GAME_AUDIENCE.RAW;
/*----------------------------------------------------------------------*/
//📓 We Have A Data Pipeline!
/*
By "data pipeline" we mean:
A series of steps...
that move data from one place to another...
in a repeated way.
*/

//📓 Automating Step 1: Agnie's Files Moved Into the Bucket
--Wanna hear some great news?  We already automated step 1 for you!!

/*
🎯 Create A New Stage and a New Target Table!
Remember, everything you create should be owned by SYSADMIN. 
1) Create a new stage called UNI_KISHORE_PIPELINE that points to s3://uni-kishore-pipeline. Put this stage in the RAW schema. 
2) Check the current time in UTC. If it is currently almost midnight UTC, expect lots of files. If it is just after midnight UTC, expect very few files. 
3) Look at the files in the new stage using LIST. Depending on the time of day you run this, the number of files you see will vary.  You might see 2, you might see hundreds. 
*/
//🥋 Use a Code Template to Create a Stage
CREATE OR REPLACE STAGE AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
	URL = 's3://uni-kishore-pipeline'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 5: Data Engineering Workshop'
;

select current_timestamp();

list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--4) Create a table called PIPELINE_LOGS (put it in the RAW schema).  It should have the same structure as the GAME_LOGS table. Same column(s) and column data type(s). 
create or replace table AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS
(
RAW_LOG VARIANT
)
COMMENT = 'Badge 5: Data Engineering Workshop';

/*
📓 Why Do the Files Disappear at Midnight UTC?
We write the same set of files every 24 hours and we needed a way to keep the bucket from getting too full.
So, every night at midnight UTC, we remove all the files and the process starts over. The first file our process writes will be named logs_1_10_0_0_0.json and it will have 10 log records in it. The second file will be called logs_11_20_0_0_0.json. 
Plan your labs with an understanding of this necessary weirdness. In the real world, the same set of files isn't written and deleted every 24 hours, but that's how we set it up for this workshop.

📓 You Are In the Process of Engineering a Pipeline
Now that we have a new stage to pull files from, and a target table for those files, we need a new COPY INTO to do the extract and load. We can re-use our existing file format. 
Use your previous COPY INTO as a template for a new COPY INTO. 

🎯 Create Your New COPY INTO
Remember, everything you create should be owned by SYSADMIN. 

1) Write a COPY INTO statement that will load not just a specific, named file into your table, but ANY file that lands in that folder. 

2) Test your COPY INTO statement and when you see the results, make a note of how many files were loaded. 

3) Look at your PIPELINE_LOGS table. How many rows does it have? Each file you load should have 10 records. Does the number of records seem correct?

4) Run your COPY INTO again. Each time you run it,  if a new file has been added, it will be loaded, if no new files are present, nothing will be loaded. 

IMPORTANT NOTE:  In line 2 of your COPY INTO command do not include a folder or filename. Just put either: 

FROM @uni_kishore_pipeline , or
FROM @ags_game_audience.raw.uni_kishore_pipeline
and the command will pick up every available file and try to load it! You should not specify a file name. 
*/

//🎯 Create Your New COPY INTO
COPY INTO AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
FILE_FORMAT = (FORMAT_NAME = 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
ON_ERROR=ABORT_STATEMENT
;

//validation
select * from AGS_GAME_AUDIENCE.INFORMATION_SCHEMA.LOAD_HISTORY where schema_name ='RAW' and table_name='PIPELINE_LOGS' order by last_load_time desc;

/*
 📓 Idempotent COPY INTO
So, did you notice that the COPY INTO is smart enough to know which files it already loaded and it doesn't load the same file, twice?
Snowflake is designed like this to help you. Without any special effort on your part, you have a process that doesn't double-load files.  In other words, it automatically helps you keep your processes IDEMPOTENT.
But, what if, for some crazy reason, you wanted to double-load your files? 
You could add a FORCE=TRUE; as the last line of your COPY INTO statement and then you would double the number of rows in your table. 
Then, what if you wanted to start over and load just one copy of each file?
You could TRUNCATE TABLE PIPELINE_LOGS; , set FORCE=FALSE and run your COPY INTO again. 
The COPY INTO is very smart, which makes it useful and efficient!! We aren't going to use the FORCE command in this workshop. We aren't going to truncate and reload to prove the stage and COPY INTO are colluding in your favor (they really do!), but we wanted you to know they are available to you for special situations. 
*/
// 📓 Another Method (Very Cool) for Getting Template Code
//🎯 Create a New LOGS View
/*Remember, everything you create should be owned by SYSADMIN. 
1) Using your LOGS View as a template, create a new View called PL_LOGS. The new view should pull from the new table.
2) Check the new view to make sure all your rows appear in your new view as you expect them to. 
*/
create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS
COMMENT = 'Badge 5: Data Engineering Workshop'
as
select
-- RAW_LOG:agent::string as agent
 RAW_LOG:user_event::string as user_event
,RAW_LOG:user_login::string as user_login
,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
,RAW_LOG:ip_address::text as ip_address
,RAW_LOG
from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS
where ip_address is not null
;

select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS;
select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS where user_login ilike '%Prajina%';

//🎯 Modify the Step 4 MERGE Task !
--Look at the code you used in your Merge Task, LOAD_LOGS_ENHANCED. 
--Does any of the code need to be changed to make it work with the PL_LOGS view instead of the old LOGS view?  If so, change it. 
select get_ddl('table','AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED');
create or replace TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED (
	IP_ADDRESS VARCHAR(16777216),
	GAMER_NAME VARCHAR(16777216),
	GAME_EVENT_NAME VARCHAR(16777216),
	GAME_EVENT_UTC TIMESTAMP_NTZ(9),
	CITY VARCHAR(16777216),
	REGION VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	GAMER_LTZ_NAME VARCHAR(16777216),
	GAME_EVENT_LTZ TIMESTAMP_NTZ(9),
	DOW_NAME VARCHAR(3),
	TOD_NAME VARCHAR(25)
);

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
--from AGS_GAME_AUDIENCE.RAW.LOGS as logs
from AGS_GAME_AUDIENCE.RAW.PL_LOGS as logs
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
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

//📓 Checking Task Run History
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),
    task_name=>'LOAD_LOGS_ENHANCED'));


//🎯 Create a Step 2 Task to Run the COPY INTO
/*Remember, everything you create should be owned by SYSADMIN. 
Create a Task that runs every 5 minutes. Name your task GET_NEW_FILES (put it in the RAW schema)
Copy and paste your COPY INTO into the body of your GET_NEW_FILES task. 
Run the EXECUTE TASK command a few times. New files are being added to the stage every 5 minutes, so keep that in mind as you test.  
Check to confirm that your task is running successfully and that the data from the files is being loaded as you expect. 
*/
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '5 MINUTE'
AS
COPY INTO AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
FILE_FORMAT = (FORMAT_NAME = 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
ON_ERROR=ABORT_STATEMENT
;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

--check to see how many rows were added
select count(*) from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS; --1900
select * from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;

//📓 Checking Task Run History
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('day',-1,current_timestamp()),
    task_name=>'GET_NEW_FILES'));

// 📓  Allowing Our Task to Run Itself
//🎯 Create a Resource Monitor to Shut Things Down After an Hour of Use
USE ROLE accountadmin;  
CREATE RESOURCE MONITOR IF NOT EXISTS IDENTIFIER('"DAILY_SHUTDOWN"') CREDIT_QUOTA = 1 FREQUENCY = 'DAILY' START_TIMESTAMP = 'IMMEDIATELY' TRIGGERS ON 75 PERCENT DO SUSPEND ON 98 PERCENT DO SUSPEND_IMMEDIATE ON 50 PERCENT DO NOTIFY
ALTER ACCOUNT SET RESOURCE_MONITOR = 'DAILY_SHUTDOWN';

//🎯 Truncate The Target Table
--Before we begin testing our new pipeline, TRUNCATE the target table ENHANCED.LOGS_ENHANCED so that we don't have the rows from our previous pipeline. Starting with zero rows gives us an easier way to check that our new processes work the way we intend. 
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

//📓 The Current State of Things
--Our process is looking good. We have:
--Step 1 TASK (invisible to you, but running every 5 minutes)
--Step 2 TASK that will load the new files into the raw table every 5 minutes (as soon as we turn it on).
--Step 3 VIEW that is kind of boring but it does some light transformation work for us.  
--Step 4 TASK that will load the new rows into the enhanced table every 5 minutes (as soon as we turn it on).

//🥋 Turn on Your Tasks!
--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

//📓  Relax!
//The team can relax now. The data load is automated!

//❕You Have Tasks Running!
--You have tasks running right now.
--Remember to always shut off your tasks when you quit your learning for the day.
--A Resource Monitor can protect your free credits if you forget to shut them off. 
--Keep this code handy for shutting down the tasks each day
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

//🥋 Let's Check Our Tasks
/*
Navigate to the LOAD_LOGS_ENHANCED Task's page.
Note whether your TASK is owned by SYSADMIN, and whether it is running. If it is SCHEDULED, note the time it will next run.
Refresh the page after it has run again. Check to see if the TASK succeeded.
NOTE: If the task is not owned by SYSADMIN, you will have to SUSPEND it, change the ownership and then RESUME it. If the task is not running, run the ALTER command that ends in RESUME.
*/

//🎯 Check on the GET_NEW_FILES Task
//Use the same methods to check on your other scheduled task. Make sure it is running and succeeding!

//🏆 Keeping Tallies in Mind
/*
A good Data Engineer will constantly be thinking about how many rows they expect so that if something weird happens, they will recognize it sooner. 
STEP 1: Check the number of files in the stage, and multiply by 10. This is how many rows you should be expecting. 
STEP 2: The GET_NEW_FILES task grabs files from the UNI_KISHORE_PIPELINE stage and loads them into PIPELINE_LOGS. How many rows are in PIPELINE_LOGS? 
STEP 3: The PL_LOGS view normalizes PIPELINE_LOGS without moving the data. Even though there are some filters in the view, we don't expect to lose any rows. How many rows are in PL_LOGS?
STEP 4: The LOAD_LOGS_ENHANCED task uses the PL_LOGS view and 3 tables to enhance the data. We don't expect to lose any rows. How many rows are in LOGS_ENHANCED?
NOTE: If you lose records in Step 4, it could be because the time zone lookup against IPINFO_GEOLOC failed. These records losses are considered acceptable in this phase of the project. If you needed to check to see if that was the reason for losing records, how would you go about checking it? Post your ideas in the 🧠 Brainstorm discussion board below. 
*/
 

//🥋 Checking Tallies Along the Way
--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;

--Step 3 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

/*
📓 A Few Task Improvements
As you were tracing your results through all the locations, did it occur to you that the timing could mess up your tallies? What if a file had just been added to the bucket, but had not been picked up by the GET_NEW_FILE task? What if some rows had been processed by the GET_NEW_FILES task but had not yet been processed by the LOAD_LOGS_ENHANCED task? 

TASK DEPENDENCIES
One way we can improve this is through task dependencies. You can't control the Step 1 task -- in fact, you don't even know the name of it. But the Step 2 Task and the Step 4 Task are yours and you have full control over them. 
What if we ran GET_NEW_FILES every 5 minutes and then ran LOAD_LOGS_ENHANCED based on Snowflake telling us that GET_NEW_FILES just finished? That would remove some of the uncertainty. 
We'll make those changes in a moment - but before we do, let's talk about one other change. 

SERVERLESS COMPUTE
The WAREHOUSE we are using to run the tasks has to spin up each time we run the task. Then, if it's designed to auto-suspend in 5 minutes, it won't EVER suspend, because the task will run again before it has time to shut down. This can cost a lot of credits.
Snowflake has a different option called "SERVERLESS". It means you don't have to spin up a warehouse, instead you can use a thread or two of another compute resource that is already running. Serverless compute is much more efficient for these very small tasks that don't do very much, but do what they do quite often.  
To use the SERVERLESS task mode, we'll need to grant that privilege to SYSADMIN. 
*/

//🥋 Grant Serverless Task Management to SYSADMIN
use role accountadmin;
grant execute managed task on account to sysadmin;
use role sysadmin;

//🥋 Replace the WAREHOUSE Property in Your Tasks
//🥋 Replace or Update the SCHEDULE Property
/*
Use one of these lines in each task. Make sure you are using the SYSADMIN role when you replace these task definitions.  
--Change the SCHEDULE for GET_NEW_FILES so it runs more often
schedule='5 Minutes'
--Remove the SCHEDULE property and have LOAD_LOGS_ENHANCED run  
--each time GET_NEW_FILES completes
after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
*/
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '5 MINUTE'
AS
COPY INTO AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
FILE_FORMAT = (FORMAT_NAME = 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
ON_ERROR=ABORT_STATEMENT
;

CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
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
--from AGS_GAME_AUDIENCE.RAW.LOGS as logs
from AGS_GAME_AUDIENCE.RAW.PL_LOGS as logs
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

//🎯 Resume the Tasks
/*Remember that each time you replace the task, you have to run an ALTER statement to RESUME the task. 
When you have tasks that are dependent on other tasks, you must resume the dependent tasks BEFORE the triggering tasks. Resume LOAD_LOGS_ENHANCED first, then resume GET_NEW_FILES. 
FYI: The first task in the chain is called the Root Task. In our case, GET_NEW_FILES is our Root Task. 
Once you have resumed the tasks, check on their status. Including using the Graph Tab to see the new dependency you created!!
*/
show tasks in schema AGS_GAME_AUDIENCE.RAW;

--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;
--
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

//🎯 Allow Your Tasks to Succeed, Then Suspend Them
/*
Once you've seen the new versions of the tasks succeed, you can SUSPEND both tasks. 
If you are stopping your learning for today, suspend your tasks, now (you can restart them next time you sit down to learn).
REMEMBER: It is your responsibility to protect your free trial credits. If you squander your credits and run out before completing the badge requirements, you will have to start over with a new trial account, or enter a credit card to finish the workshop. 
*/
select max(tally) from (
       select CASE WHEN SCHEDULED_FROM = 'SCHEDULE' 
                         and STATE= 'SUCCEEDED' 
              THEN 1 ELSE 0 END as tally 
   from table(ags_game_audience.information_schema.task_history (task_name=>'GET_NEW_FILES')));






