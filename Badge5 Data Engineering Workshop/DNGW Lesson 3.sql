//Lesson 3: Time Zones, Dates and Timestamps  📓 Time Zones Refresher!

use role sysadmin;
use warehouse compute_wh;
use schema AGS_GAME_AUDIENCE.RAW;
/*----------------------------------------------------------------------*/
//📓  Time Zones Around the World
/*
Kishore runs the command SELECT current_timestamp(); in a worksheet (in October) and sees -0600 as part of the results.

-0600 is the same thing as UTC-6.

This means Kishore's Snowflake session is currently using the Denver time zone. 

What time zone is your Snowflake Trial Account using?  Run the current_timestamp() command to find out. Our guess is that you'll see either UTC-7 (-0700) or UTC-8 (-0800) depending on the time of year it is (daylight savings time).

We can guess this because all Snowflake Trial Account use "America/Los_Angeles" as the default. This may be because Snowflake was founded in San Mateo, California, USA. 
*/
select current_timestamp();

//🥋 Change the Time Zone for Your Current Worksheet
--What time zone is your account(and/or session) currently set to? is it -0700?
select current_timestamp(); --2024-01-29 08:22:08.692 -0800

--worksheets are sometimes called sessions --we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';

//📓 Time Zones in Agnie's Data
select * from AGS_GAME_AUDIENCE.RAW.LOGS; 

/*📓 How Can We Find Out What the Z Means in Agnie's Data? 
The team needs to find out how the datetime data is being captured.  There are common methods for learning about source data. 

Ask someone who knows, or might know. 
Look at some documentation, somewhere.
Create your own test records and compare what you know to what flows through. 
Most teams will use a combination of all three of the methods. Sometimes a team will use one method to start, and another to confirm.
*/

//🖼️ Agnie Downloads an Updated Log File!
/*
After confirming with Tsai and Kishore on Discord, Agnie adds IP_ADDRESS to the list of fields in the feed and removes AGENT. Then, she outputs a new file.

Kishore already gave her read/write access to his S3 bucket (uni-kishore), so she creates a folder named "updated_feed" and loads the new file into it. 

Now it's time for Kishore (and YOU!) to check for that new file, view the records pre-load ($1!), load them, and view them again using the LOGS view. 
*/

/*
🎯 CHALLENGE: Update Your Process to Accommodate the New File
Find the new file Agnie downloaded from the game platform by listing files in the stage you already set up. Agnie put it in a different folder. It's not in the "kickoff" folder. 
Assess whether the GAME_LOGS table will need to be modified to accommodate the added IP_ADDRESS field. 
If GAME_LOGS table needs to be changed, change it. 
Load the file into the GAME_LOGS table. 

TIPS
Do not remove the old rows (or if you do remove them by accident, re-load them). 
Remember that our previous load was done with a COPY INTO pointed at the folder "kickoff." This new file is in a different folder. 
Look at the data in the GAME_LOGS table after you load it. Understand how the second set of rows differs from the first set that was loaded. 
*/
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE;

COPY INTO AGS_GAME_AUDIENCE.RAW.GAME_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/updated_feed/
--FILES = ('DNGW_Sample_from_Agnies_Game.json')
FILE_FORMAT = (FORMAT_NAME = 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')
ON_ERROR=ABORT_STATEMENT
;
--284

select * from AGS_GAME_AUDIENCE.RAW.GAME_LOGS; --534
select RAW_LOG:agent::text,RAW_LOG:ip_address::text from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

//🎯 CHALLENGE: Filter Out the Old Rows
select * from AGS_GAME_AUDIENCE.RAW.GAME_LOGS where RAW_LOG:ip_address::text is not null;

select
 RAW_LOG:agent::string as agent
,RAW_LOG:ip_address::text as ip_address
,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
,RAW_LOG:user_event::string as user_event
,RAW_LOG:user_login::string as user_login
,RAW_LOG
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
where ip_address is not null
;

//🥋 Two Filtering Options

--looking for empty AGENT column
select * 
from ags_game_audience.raw.LOGS
where agent is null;

--looking for non-empty IP_ADDRESS column
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,*
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

/*
🎯 CHALLENGE: Update Your LOG View
Change the LOG view definition so that it no longer contains an AGENT column. 
Change the LOG view definition so that it now contains the IP_ADDRESS column. 
Add a WHERE clause that will remove the first set of records from the view results. Do NOT remove the rows from the table. 
TIPS

If you remove the old rows by accident, re-load them. 
The order of the columns doesn't matter. 
After the changes, your results should look like this: 
*/
create or replace view AGS_GAME_AUDIENCE.RAW.LOGS
COMMENT = 'Badge 5: Data Engineering Workshop'
as
select
-- RAW_LOG:agent::string as agent
 RAW_LOG:user_event::string as user_event
,RAW_LOG:user_login::string as user_login
,RAW_LOG:datetime_iso8601::timestamp_ntz as datetime_iso8601
,RAW_LOG:ip_address::text as ip_address
,RAW_LOG
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
where ip_address is not null
;

select * from AGS_GAME_AUDIENCE.RAW.LOGS;

select * from AGS_GAME_AUDIENCE.RAW.LOGS where user_login ilike '%Prajina%';

/*🏁 Ready to Mark Lessons 3 Complete? 
If you have: 
A LOGS view that returns 284 rows: Yes
A LOGS view that returns IP_ADDRESS as a column: Yes
A LOGS view that does NOT return AGENT as a column: Yes
You should mark this lesson complete!
*/













