//🥋 Create a Warehouse for Loading INTL_DB
CREATE OR REPLACE WAREHOUSE ACME_WH
WITH
WAREHOUSE_SIZE = 'XSMALL'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 60 //60 SECONDS/1 MINUTE
AUTO_RESUME = TRUE
;


//🥋 Rename the Database You Got From the Share
use role accountadmin;
alter database GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI rename to weathersource;

//❔ What Countries are Included in the HISTORY_DAY View?
use role sysadmin;
select count(postal_code) from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY;
select * from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY limit 100;
select distinct country from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY;

//❔ What Postal Codes Are Available from the Detroit Area?
select distinct postal_code from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY 
where country = 'US'
and (startswith(postal_code,'481') or startswith(postal_code,'482'))
order by postal_code;

--OR--

select distinct postal_code from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY 
where country = 'US'
and left(postal_code, 3) in ('481','482')
order by postal_code;

//🎯 Convert Your Postal Code Query to a View
CREATE DATABASE IF NOT EXISTS IDENTIFIER('"MARKETING"') COMMENT = '';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('"MARKETING"."MAILERS"') COMMENT = '';

create or replace view MARKETING.MAILERS.DETROIT_ZIPS
as
select distinct postal_code from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY 
where country = 'US'
and left(postal_code, 3) in ('481','482')
order by postal_code
;

//❔ How Can I Filter the Data and How Much Data Can I Remove With a Filter?
/*Did you know that the view you just created can be used in a JOIN to effectively FILTER any table you join the view with? Can you use the new view to filter the HISTORY_DAY table?*/ 
select
count(hd.postal_code)
from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY as hd
join MARKETING.MAILERS.DETROIT_ZIPS as dz
on hd.postal_code = dz.postal_code
;

//❔ What's the Data Range on this Data Set?
select * from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY limit 100;
select min(DATE_VALID_STD), max(DATE_VALID_STD) from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY; --2022-01-20|2024-01-19
select
 min(hd.DATE_VALID_STD)
,max(hd.DATE_VALID_STD)
from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY as hd
join MARKETING.MAILERS.DETROIT_ZIPS as dz
on hd.postal_code = dz.postal_code
;
--2022-01-20 | 2024-01-19

//❔ Can the Data Tell Me Which Day in the Next 2 Weeks Would Be Best for a Sale? 
--This is the last query you'll write for this "Caden Explores the Data" lesson. It's not easy, but we think you can do it!! 
select * from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY limit 100; //AVG_CLOUD_COVER_TOT_PCT

select
hd.DATE_VALID_STD, avg(hd.AVG_CLOUD_COVER_TOT_PCT) 
from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY as hd
join MARKETING.MAILERS.DETROIT_ZIPS as dz
on hd.postal_code = dz.postal_code
group by hd.DATE_VALID_STD
order by avg(hd.AVG_CLOUD_COVER_TOT_PCT)
;


/*
❕❕❕ The Biggest Lesson Caden Learned...
...is that ACME needs MORE WEATHER DATA to do even MORE COOL ANALYSES!  For starters, she wants to get the Windsor, Ontario, Canada postal codes as well as the Detroit, Michigan, USA codes!!
*/




















