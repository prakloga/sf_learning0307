//Lesson 2: Project Kick-Off and Database Set Up  🎭 The Project Kick-Off Meeting
/*
Every Saturday morning, Kishore and Agnieszka play pick-up basketball. Kishore's best friend Tsai, plays in the games, too.  After their games, they go to their favorite smoothie shop. This week, they plan to brainstorm about their project.

On their way to the smoothie shop, Agnie uses her phone to send Kishore a log file she wants to discuss. Kishore downloads the file to a cloud folder.
You should download the file, too:  DNGW_Sample_from_Agnies_Game.json You can save it to your local machine. 
*/

//🎭 Deciding on Project Roles
/*
Kishore: Data Engineer
Agnieszka: Game Developer
Tsai: Business System Analyst/Project Manager
*/

/*🎯 Create the Project Infrastructure
Use SYSADMIN.
Create a database named AGS_GAME_AUDIENCE
Drop the PUBLIC schema.
Create a schema named RAW.
Double check everything. Did you name each item correctly? If not, use the ALTER statement to rename them.

Did you use SYSADMIN when creating things? If not, transfer the ownership of each object so it will be owned by the SYSADMIN role. 
*/
CREATE DATABASE IF NOT EXISTS IDENTIFIER('"AGS_GAME_AUDIENCE"') COMMENT = 'Badge 5: Data Engineering Workshop';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('"AGS_GAME_AUDIENCE"."RAW"') COMMENT = 'Badge 5: Data Engineering Workshop';
DROP SCHEMA IF EXISTS IDENTIFIER('"AGS_GAME_AUDIENCE"."PUBLIC"');

//🥋 Use a Code Template to Create a Table
--Snowflake has some code templates available that can help you when creating objects. 
create or replace table AGS_GAME_AUDIENCE.RAW.GAME_LOGS
(
RAW_LOG VARIANT
)
COMMENT = 'Badge 5: Data Engineering Workshop';

//🥋 Use a Code Template to Create a Stage
CREATE OR REPLACE STAGE AGS_GAME_AUDIENCE.RAW.UNI_KISHORE
	URL = 's3://uni-kishore'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 5: Data Engineering Workshop'
;

//🥋 Test the Stage
--Remember that a LIST command is a great way to make sure your stage is working and to get the names of files within the stage. 
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE;

/*
🎯 Create a File Format
Now you that you know how to find and use code templates, you can use a code template to create a file format. 

Use SYSADMIN.
Create a File Format in the AGS_GAME_AUDIENCE.RAW schema named FF_JSON_LOGS.
 Set the data file Type to JSON 
  Set the Strip Outer Array Property to TRUE   (strip_outer_array = true)
*/
create or replace file format AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS
type = 'JSON'
COMPRESSION = 'AUTO'
STRIP_OUTER_ARRAY = TRUE
COMMENT = 'Badge 5: Data Engineering Workshop'
;

//📓 Exploring the File Before Loading It
select
$1
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/kickoff/
(file_format => AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS)
;

//🥋 Load the File Into The Table
/*You've also got lots of experience writing COPY INTO statements, and all the needed pieces (stage, table, file format) are in place, so write your COPY INTO statement and load the file into the table. 
Did you notice that we did not write out the file name in the FROM line? This is because there is only one file in the kickoff folder. 
A COPY INTO statement like the one shown above will load EVERY file in the folder if more than one file is there, and the file name is not specified. This will come in very handy later in the course. 

There are other ways to specify what files should be loaded and Snowflake gives you a lot of tools to further specify what will be loaded, but for now accept the general rule that by not naming the file, you are asking SNOWFLAKE to attempt to load ALL files the stage or stage/folder location. 
*/

COPY INTO AGS_GAME_AUDIENCE.RAW.GAME_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/kickoff/
--FILES = ('DNGW_Sample_from_Agnies_Game.json')
FILE_FORMAT = (FORMAT_NAME = 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
ON_ERROR=ABORT_STATEMENT
;

//🥋 Build a Select Statement that Separates Every Attribute into It's Own Column
--The code shown here should get you started on your select statement, but you'll need to add to it.
select * from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

/*
Remember the JSON parsing PATHS and data type CASTING we learned in Badge 1?  Use those techniques to build a SELECT statement that separates every field in the RAW_LOG column into it's own column of the SELECT results. 
The order of columns is not important.   For the column that contains data and time information, cast it to TIMESTAMP_NTZ. 
Include the original column RAW_LOG as the last column. We always like to be able to refer back to the original JSON so carrying this field forward is a good idea. 
When your SELECT is complete, you should have 5 columns. 
*/
select
 RAW_LOG:agent::string as agent
,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
,RAW_LOG:user_event::string as user_event
,RAW_LOG:user_login::string as user_login
,RAW_LOG
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
;

/*🎯 Create Your View
Use SYSADMIN.
Create a view named LOGS in the RAW schema. 
Run a SELECT * on your new view and make sure it returns all 250 rows. Additionally, make sure your view has the same column headings shown here, and your data is formatted the same. The order of the columns doesn't matter but if other things don't look right, you'll need to make adjustments to your view definition. 
*/
create or replace view AGS_GAME_AUDIENCE.RAW.LOGS
COMMENT = 'Badge 5: Data Engineering Workshop'
as
select
 RAW_LOG:agent::string as agent
,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
,RAW_LOG:user_event::string as user_event
,RAW_LOG:user_login::string as user_login
,RAW_LOG
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
;

select * from AGS_GAME_AUDIENCE.RAW.LOGS;
select count(*)  
from ags_game_audience.raw.logs
where is_timestamp_ntz(to_variant(datetime_iso8601))= TRUE ;

select datetime_iso8601, to_variant(datetime_iso8601), is_timestamp_ntz(to_variant(datetime_iso8601)) from ags_game_audience.raw.logs;

/*
 🏁 Ready to Mark Lesson 2 Complete? 

Are all these statements true for you?

You have a database named AGS_GAME_AUDIENCE: Yes
Your database has two schemas: INFORMATION_SCHEMA and RAW: Yes
Your database and RAW schema are both owned by the SYSADMIN role: Yes
You have a table in the RAW schema named GAME_LOGS: Yes
Your GAME_LOGS table has a single VARIANT column and 250 rows loaded.: Yes
You have a view named LOGS in the RAW schema that surfaces all 250 rows from the underlying table (GAME_LOGS).: Yes
Your table and view are both owned by the SYSADMIN role.: Yes
If all these statement are true for your Snowflake Trial Account, you should mark this lesson complete!
*/


