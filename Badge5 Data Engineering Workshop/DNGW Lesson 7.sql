//Lesson 7: DE Practice Improvement & Cloud Foundations  📓 Resting or Revving Up? 
use role sysadmin;
use warehouse compute_wh;
use schema AGS_GAME_AUDIENCE.RAW;
/*----------------------------------------------------------------------*/
//📓 Data Engineer Skillset Improvements
//📓 Pipeline Improvements
/*
His coworkers suggest the following improvements:
He could add some file metadata columns to the load so that he will have a record of what files he loaded and when. 
He could move the logic from the PL_LOGs view into the same select. 
If he does change the select logic, he will then need a new target table to accommodate the output of the new select. 
When he has a new select that matches the new target table, he can put it into a new COPY INTO statement. 
After he has a new COPY INTO, he could put it into an Event-Driven Pipeline (instead of a task-based Time-Driven Pipeline)
*/

//🥋 A New Select with Metadata and Pre-Load JSON Parsing 
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

select
$1
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(FILE_FORMAT => 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
;

select
 METADATA$FILENAME as log_file_name --new metadata column
,METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
,current_timestamp(0) as load_ltz --new local time of load
,$1
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(FILE_FORMAT => 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
;

select
 METADATA$FILENAME as log_file_name --new metadata column
,METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
,current_timestamp(0) as load_ltz --new local time of load
,$1:datetime_iso8601::timestamp_ntz as datetime_iso8601
,$1:user_event::text as user_event
,$1:user_login::text as user_login
,$1:ip_address::text as ip_address
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(FILE_FORMAT => 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
;

--OR--

select
 METADATA$FILENAME as log_file_name --new metadata column
,METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
,current_timestamp(0) as load_ltz --new local time of load
,get($1,'datetime_iso8601')::timestamp_ntz as datetime_iso8601
,get($1,'user_event')::text as user_event
,get($1,'user_login')::text as user_login
,get($1,'ip_address')::text as ip_address
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(FILE_FORMAT => 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
;

//🎯 Create a New Target Table to Match the Select  (Using CTAS, if you want to)
/*
You're going to create a new logs table in the RAW schema and call it ED_PIPELINE_LOGS. You could write this table definition any way you want. Use a template, write it manually, or use a CTAS. 
To create the table using CTAS, simply type CREATE TABLE <table name> AS on the line above your SELECT statement, and run it.
Using CTAS creates the table and loads it in one step. If you create the table another way, you will need to write an INSERT statement using the SELECT and run that, too.
After creating the new table and loading it, go look at your table and the rows in it to see what you think of it. 
*/
create or replace table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
COMMENT = 'Badge 5: Data Engineering Workshop'
as
select
 METADATA$FILENAME as log_file_name --new metadata column
,METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
,current_timestamp(0) as load_ltz --new local time of load
,$1:datetime_iso8601::timestamp_ntz as datetime_iso8601
,$1:user_event::text as user_event
,$1:user_login::text as user_login
,$1:ip_address::text as ip_address
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(FILE_FORMAT => 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
;

//Validation
select * from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

//📓 Next Up: The Improved COPY INTO
--truncate the table rows that were input during the CTAS
truncate table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
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

SELECT * FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

//📓 Developing Confidence as a Data Engineer
//📓 Event-Driven Pipelines
//📓 Review, Progress, and Next Steps
//📓 Cloud-Based Services for Modern Data Pipelines
//📓 A Closer Look at Pub/Sub Services










