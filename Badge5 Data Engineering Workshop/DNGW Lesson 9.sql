//Lesson 9: Curated Data  🎯 Curated Data 
use role sysadmin;
use warehouse compute_wh;
use schema AGS_GAME_AUDIENCE.RAW;
/*----------------------------------------------------------------------*/
//🎯 Create a CURATED Layer
--Create a SCHEMA named CURATED in the AGS_GAME_AUDIENCE database.
--Make sure the schema is owned by SYSADMIN.
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('"AGS_GAME_AUDIENCE"."CURATED"') COMMENT = 'Badge 5: Data Engineering Workshop';

//📓 Snowflake Dashboards
//🥋 Create a New Dashboard and Add a Tile
//🥋 Rolling Up Login and Logout Events with ListAgg
--the List Agg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select
 gamer_name
,listagg(game_event_ltz,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
group by gamer_name
;

// 🥋 Windowed Data for Calculating Time in Game Per Player
select
 gamer_name
,game_event_ltz as login
,lead(game_event_ltz) over(partition by gamer_name order by game_event_ltz) as logout
,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc
;

//🎯 Add a Heatmap for Session Length x Time of Day
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length >= 10 and game_session_length < 20 then '10 to 19 mins'
            when game_session_length >= 20 and game_session_length < 30 then '20 to 29 mins'
            when game_session_length >= 30 and game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins'
            end as session_length
,tod_name
from(
select
 gamer_name
,tod_name
,game_event_ltz as login
,lead(game_event_ltz) over(partition by gamer_name order by game_event_ltz) as logout
,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc
)t
;


//🥋 Code for the Heatmap
--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length >= 10 and game_session_length < 20 then '10 to 19 mins'
            when game_session_length >= 20 and game_session_length < 30 then '20 to 29 mins'
            when game_session_length >= 30 and game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED)
where logout is not null;








