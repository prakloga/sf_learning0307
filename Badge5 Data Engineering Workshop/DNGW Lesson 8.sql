//Lesson 8: Your Snowpipe!  🥋 Time to Set Up YOUR Snowpipe! 
use role sysadmin;
use warehouse compute_wh;
use schema AGS_GAME_AUDIENCE.RAW;
/*----------------------------------------------------------------------*/
//📓  The Power of Hub & Spoke Pub/Sub Systems
//🥋 Create Your Snowpipe!
CREATE OR REPLACE PIPE AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
auto_ingest=TRUE
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
FROM (select
 METADATA$FILENAME as log_file_name --new metadata column
,METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
,current_timestamp(0) as load_ltz --new local time of load
,$1:datetime_iso8601::timestamp_ntz as datetime_iso8601
,$1:user_event::text as user_event
,$1:user_login::text as user_login
,$1:ip_address::text as ip_address
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
FILE_FORMAT = (FORMAT_NAME='AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
;

select count(*) from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--if you need to pause or unpause your pipe
alter pipe GET_NEW_FILES set pipe_execution_paused = true;
alter pipe GET_NEW_FILES set pipe_execution_paused = false;


//📓  Our Event-Driven Pipeline Progress
/*
We've got one more step to complete our Event-Driven Pipeline. We need to update our LOAD_LOGS_ENHANCED task so that it loads from the ED_PIPELINE_LOGS table instead of the PIPELINE_LOGS table. 
To finalize the Event-Driven pipeline, we need to edit the existing LOAD_LOGS_ENHANCED task we set up earlier and RESUME it! 
*/

//🎯 Update the LOAD_LOGS_ENHANCED Task
/*
Edit the Task so it loads from ED_PIPELINE_LOGS instead of PIPELINE_LOGS. If the task is running, you'll need to suspend it. If the GET_NEW_FILES task is currently running, you'll have to suspend it before you can replace it with an updated version. 
Update the task to run every 5 minutes
Resume the task.  (Remember to SUSPEND THE TASK before you stop for the day and turn it back on when you resume next time)
NOTE: You now have both one PIPE and one TASK running! Keep this in mind and if your Resource Monitor blocks you, go in and edit it to allow for 2 credit hours today, or even 3!
*/
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
--from AGS_GAME_AUDIENCE.RAW.PL_LOGS as logs
from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS as logs
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

//🥋 Turn on Your Tasks!
--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

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

//📓 Fully Event-Driven?
/*
You probably noticed that our new Pipe did not make our entire pipeline "event driven." We still have a time-based process writing files to the bucket. 
We also still have a time-based task moving data from our ED_PIPELINE_LOGS table into our LOGS_ENHANCED table.
What's most important is that you now have hands-on experience with two of the most important pipeline devices in Snowflake: tasks and pipes.
There are a few more improvements we can make to our pipeline. One of those improvements is called a STREAM.

STREAMS can get VERY sophisticated and complex. The STREAM you are about to create will be very, very basic. 
To effectively use STREAMS in your work, you will want to get more advanced Snowflake training or read up and experiment on your own.  
For now, we include STREAMS so that you will know they exist and have the most basic understanding of how they work.  Below is a diagram of the STREAM we will add to our latest pipeline. 
The diagram above shows that STREAM will not replace that last task and it will not make it event-driven, but it will make the pipeline more efficient. It will do this by allowing us to use a technique called "Change Data Capture" which is why the diagram is labeled with "CDC."
Let's start by adding the STREAM. 
*/
//🥋 Create a Stream
--create a stream that will keep track of changes to the table
create or replace stream AGS_GAME_AUDIENCE.RAW.ED_CDC_STREAM
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS 
--SHOW_INITIAL_ROWS = TRUE
;

--look at the stream you created
show streams;

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

//📓 Streams Can Be VERY Complex - Ours is Simple
//🥋 View Our Stream Data
--query the stream
select * from ags_game_audience.raw.ed_cdc_stream; 
select count(*) from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('GET_NEW_FILES');



//📓 Processing Our Simple Stream
//🥋 Process the Rows from the Stream
--make a note of how many rows are in the stream
select * from ags_game_audience.raw.ed_cdc_stream; 

--process the stream by using the rows in a merge 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

--Did all the rows from the stream disappear? 
select * 
from ags_game_audience.raw.ed_cdc_stream; 

//📓 The Final Task in Our Pipeline - Ripe For Improvement
//🥋 Create a CDC-Fueled, Time-Driven Task
--turn off the other task (we won't need it anymore)
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED suspend;

//📓 A Final Improvement!
/*
Let's add one more piece of Data Engineering sophistication to the Pipeline. It won't improve our load costs, because our files are going to load every 5 minutes by design, 
but if you have a truly event-driven pipeline on the front end, this last enhancement can make a difference in the last step of your pipeline. 

We're going to add a WHEN clause to the TASK that checks the STREAM. It will still try to run every 5 minutes, but if nothing has changed, it won't continue running. 
*/
//🎯 Add A Stream Dependency to the Task Schedule
--Add STREAM dependency logic to the TASK header and replace the task. 
--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
WHEN
    system$stream_has_data('ed_cdc_stream')
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

--Truncate the target table. 
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Check that the target table is empty. 
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED suspend;

--Check that the target table is loading again. 
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;





























