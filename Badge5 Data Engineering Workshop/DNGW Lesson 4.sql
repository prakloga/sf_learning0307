//Lesson 4: Extracting, Transforming, and Loading  📓 ETL? or ELT? 
use role sysadmin;
use warehouse compute_wh;
use schema AGS_GAME_AUDIENCE.RAW;
/*----------------------------------------------------------------------*/
//📓 Extracting, Transforming and Loading
//📓 Defining the Transformed State
/*
Data Engineers often perform a series of ETL steps and so they have different "layers" where data is expected to have reached certain levels of refinement or transformation. In this workshop we'll have named our layers: 
RAW
ENHANCED
CURATED
In this next step, Kishore will try to make the RAW data more valuable than it is currently, by adding information that will ENHANCE that data. 
*/
//📓 A Project Status Meeting
//📓 Next Steps
/*
The team discusses alternative ways to get time zone information for each gamer.  Kishore notes that IP Addresses can be geo-located and that geolocation can be used to infer a time zone. 
Agnie mentions that the use of VPNs can mess up IP Geolocation. That's a problem, but the team agrees that using the IP address, even with the VPN issue, is better than not having any time zone information at all. 
Tsai asks Kishore how he plans to perform the IP Geolocation process.  Kishore says there are lots of options. He notes that there are lookup API services available.
Then, Tsai asks if anyone has checked the Marketplace to see if any companies are offering IP Address-based time zone look up via a share. Kishore quickly searches the Marketplace and finds a listing from a company called IPInfo. The sample data is free and the team can look up at least some of their gamers' locations. When they're sure it will work, they can talk to IPInfo about premium data. 

Kishore's first data TRANSFORMATION will be to ENHANCE the log data by adding time zone to each row. 
*/
//🎯 Use Snowflake's PARSE_IP Function
--Find Kishore's Sister's log files, and copy the IP Address assigned to Kishore's VR headset. 
--Paste the IP into this code snippet, and run it. 
--select parse_ip('<ip address>','inet');
select * from AGS_GAME_AUDIENCE.RAW.LOGS where user_login ilike '%Prajina%';
select parse_ip('100.41.16.160','inet');
/*{   "family": 4,   "host": "100.41.16.160",   "ip_fields": [     1680412832,     0,     0,     0   ],   "ip_type": "inet",   "ipv4": 1680412832,   "netmask_prefix_length": null,   "snowflake$type": "ip_address" }*/

/*🎯 Pull Out PARSE_IP Results Fields
We can pull out the values from the PARSE_IP results by adding a colon and the name after the close parentheses, like this: 
select parse_ip('107.217.231.17','inet'):host;
Or this:
select parse_ip('107.217.231.17','inet'):family;
Use the IP Address that was assigned to Kishore's headset and pull out the ipv4 property. This value is just Kishore's IP Address formatted a different way. We need his IP Address in the ipv4 format because it is easier to compare to other numbers. 
*/
select parse_ip('100.41.16.160','inet'):family;

//🎯 Enhancement Infrastructure
//Create a new schema in the database and call it ENHANCED
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('"AGS_GAME_AUDIENCE"."ENHANCED"') COMMENT = 'Badge 5: Data Engineering Workshop';

//📓 Locate the IPInfo Free Sample Data

//🥋 Look Up Kishore & Prajina's Time Zone
--Look up Kishore and Prajina's Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function.
select * from IPINFO_GEOLOC.DEMO.LOCATION limit 100;

select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.DEMO.LOCATION
where parse_ip('100.41.16.160','inet'):ipv4 between start_ip_int and end_ip_int --Kishore's Headset's IP Address
;

//🥋 Look Up Everyone's Time Zone
select * from AGS_GAME_AUDIENCE.RAW.LOGS limit 100;
select * from IPINFO_GEOLOC.demo.location limit 100;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select
 logs.*
,loc.city
,loc.region
,loc.country
,loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
where parse_ip(logs.ip_address,'inet'):ipv4 between start_ip_int and end_ip_int
;

//📓 How Expensive is This? 
//🥋 View Any Query Profile
//📓 Functions As Part of the Share
/*
We are especially interested in using two of the functions IPInfo has provided to us.
The TO_JOIN_KEY function reduces the IP Down to an integer that is helpful for joining with a range of rows that might match our IP Address.
The TO_INT function converts IP Addresses to integers so we don't have to try to compare them as strings! 
*/
//🥋 Use the IPInfo Functions for a More Efficient Lookup
--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
select
 logs.ip_address
,logs.user_login
,logs.user_event
,logs.datetime_iso8601
,loc.city
,loc.region
,loc.country
,loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
on IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) between start_ip_int and end_ip_int
;

//📓 Create a Local Time Column!
/*
We have a timestamp in every row of our logs that tells us the date and time the gaming event (login or logoff) took place. 
Based on his calculations and tracking, Kishore feels confident those timestamps are in UTC+0. 
Now we have the local time zone for many of our gamers. 
These 3 pieces of information are exactly what we need to create a new column that contains the local date and time of the gaming event. 
Kishore will use a function he found on docs.snowflake.com called CONVERT_TIMEZONE. 
https://docs.snowflake.com/en/sql-reference/functions/convert_timezone#examples
CONVERT_TIMEZONE( <source_tz> , <target_tz> , <source_timestamp_ntz> )
CONVERT_TIMEZONE( <target_tz> , <source_timestamp> )
*/
//🎯 Add a Local Time Zone Column to Your Select
--Add a column called GAME_EVENT_LTZ
--After you create the new column, use the test rows created by Kishore's sister to make sure the conversion worked.
select
 logs.ip_address
,logs.user_login
,logs.user_event
,logs.datetime_iso8601
,loc.city
,loc.region
,loc.country
,loc.timezone
,CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601) as GAME_EVENT_LTZ
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
on IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) between start_ip_int and end_ip_int
;

//🖼️ Updates and Planning
//📓 Planning More Data Enhancements
/*
Agnie and Tsai are impressed and want to know what other enhancements Kishore is planning to make. Kishore explains that he wants to hear their ideas about what would be useful. 
Agnie is wanting to know what time of the day gamers are playing. Is it after school? In the evenings? Late at night? 
Tsai suggests knowing which days of the week, like weekdays versus weekends would also be helpful. 
Kishore thinks he can also figure out how long they are playing the game each time they log in and out, and asks if that seems interesting. 
Everyone agrees that these three data enhancements sound great and Kishore says he'll let them know when he's got them added. 
*/

//🎯 Add A Column Called DOW_NAME
--Use the DAYNAME function to add the DOW ("Day of Week") name as a column.  The new column should be named DOW_NAME. Be sure to use the local time zone datetime value so that you get the day in local time. 
--https://docs.snowflake.com/en/sql-reference/functions/dayname?utm_source=snowscope&utm_medium=serp&utm_term=dayname
select
 logs.ip_address
,logs.user_login
,logs.user_event
,logs.datetime_iso8601
,loc.city
,loc.region
,loc.country
,loc.timezone
,CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601) as GAME_EVENT_LTZ
,dayname(GAME_EVENT_LTZ) as DOW_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
on IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) between start_ip_int and end_ip_int
;

/*
📓 Assigning a Time of Day
Agnie wants to know what "time" of day people are playing her game. But when asking Kishore for "the time," she's not requesting a number, she wants something more like "Before Breakfast" or "After School." 
Kishore doesn't think that makes any sense because "Before Breakfast" is not reliable across cultures and age groups. Tsai, in her role as BSA, tries to facilitate the discussion to help Agnie and Kishore find a compromise. 
After some back and forth, Kishore and Agnie agree that using labels like "Early morning" and "Mid-morning," for example, are an acceptable compromise for both of them. 
Kishore asks Agnie to write out what she wants to call each portion of the day and send it to the team via email. 
*/
//🥋 Create the Table and Fill in the Values
-- Your role should be SYSADMIN
-- Your database menu should be set to AGS_GAME_AUDIENCE
-- The schema should be set to RAW

--a Look Up table to convert from hour number to "time of day name"
create or replace table AGS_GAME_AUDIENCE.RAW.time_of_day_lu
(hour number
,tod_name varchar(25)
)
;

--insert statement to add all 24 rows to the table
insert into AGS_GAME_AUDIENCE.RAW.time_of_day_lu
values
(6, 'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

//🥋 Check the Table
select * from AGS_GAME_AUDIENCE.RAW.time_of_day_lu;
--check your table to see if you loaded it properly
select tod_name, listagg(hour,',') from AGS_GAME_AUDIENCE.RAW.time_of_day_lu group by tod_name ;

//🎯 A Join with a Function
/*
To create this next data enhancement (which you will call TOD_NAME), you will need to join to our new time of day table to the tables in our existing SELECT.
Use the "hour" column of our new table as the linking point in the ON clause. Once you've linked the two tables you can send back the TOD_NAME in the results. 
HINT: You will need a function from Snowflake's Date & Time Functions group in order to make the join. Can you figure out how to get a numeric hour that you can then use to join to the hour number in the new table? 
*/
select
 logs.ip_address
,logs.user_login
,logs.user_event
,logs.datetime_iso8601
,loc.city
,loc.region
,loc.country
,loc.timezone
,CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601) as GAME_EVENT_LTZ
,dayname(GAME_EVENT_LTZ) as DOW_NAME
,tod.tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS as logs
join IPINFO_GEOLOC.demo.location as loc
on IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) between start_ip_int and end_ip_int
join AGS_GAME_AUDIENCE.RAW.time_of_day_lu as tod 
on hour(CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601)) = tod.hour
;

/*
🎯 Rename the Columns
Before we do that, let's give better names to some of our columns!  Remember you just need to put "as" between the column definition and the new name. 

 logs.user_login should be renamed to GAMER_NAME
 logs.user_event should be renamed to GAME_EVENT_NAME
 logs.datetime_iso8601 should be renamed to GAME_EVENT_UTC
timezone should be renamed GAMER_LTZ_NAME
Other columns can keep their current names. 
*/
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
on hour(CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601)) = tod.hour
;

/*
📓 Query Complexity
Our select statement is starting to get somewhat complex. It might be a good idea to take the results and move them somewhere, especially now that the data is no longer RAW, it's starting to merit being referred to as ENHANCED. 
We could wrap our select in a view, but the select is already based on a view, joined with a share and another table. It might be nice to write the data into a table to sort of lock it down.
To create the table we can use a CTAS -- a Create Table as Select. CTAS statements are a really quick way to create a table. It's not a long-term solution, but a stepping stone as we work out the process logic. 
*/
//🥋 Convert a Select to a Table
--Wrap any Select in a CTAS statement
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
on hour(CONVERT_TIMEZONE('UTC',loc.timezone,logs.datetime_iso8601)) = tod.hour
;

select * from ags_game_audience.enhanced.logs_enhanced;
select *
from ags_game_audience.enhanced.logs_enhanced
where dow_name = 'Sat'
and tod_name = 'Early evening'   
and gamer_name like '%prajina'
;

/*
🎯 Check Your Table
Find ways to check your work. Here are some ways to check your work. Do some of them or all of them. 
Can you see the table in the picker? Yes
Can you run a select star on your new table? Yes
How many rows are in your table? 146
Is that what you expected based on the number returned from earlier views? Yes
How many columns? 11
Are they named what you thought they would be named? Yes
Where is your table located? ags_game_audience.enhanced
Is it in the schema and database you intended? Yes
What role owns the table? Is it the role you intended? Sysadmin
*/































































