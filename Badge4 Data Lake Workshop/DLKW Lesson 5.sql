//Lesson 5: Mel's Concept Kickoff  📓 Geography Refresher!
use role sysadmin;
use warehouse compute_wh;
/*-----------------------------------------------------------------*/
/*
🎯 Put Your Snowflake Skills to Work!
This challenge lab is very much like the work a paid data professional will be asked to do.  If you already work as a data professional, this should be easy. If you are frustrated by the lack of step-by-step instructions in this lesson, remember that you have been creating stages, views and file formats since Workshop 1. You have also done these tasks earlier in this lab. You have the skills to complete this lab, and pulling all the steps together on your own can be very rewarding. 

Start with this set of tasks:

Make sure everything you create is owned by the SYSADMIN role. 
Create a database called MELS_SMOOTHIE_CHALLENGE_DB. 
Drop the PUBLIC schema 
Add a schema named TRAILS
Next, you'll add two stages. Camila has loaded geospatial trail files to the two folders shown below. These folders are in the trails folder of the dlkw folder of the uni-lab-files-more bucket in the AWS West 2 region. Remember that s3 is very particular about upper and lower case.  
*/
CREATE DATABASE IF NOT EXISTS IDENTIFIER('"MELS_SMOOTHIE_CHALLENGE_DB"') COMMENT = 'Badge 4: Data Lake Workshop';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('"MELS_SMOOTHIE_CHALLENGE_DB"."TRAILS"') COMMENT = 'Badge 4: Data Lake Workshop';
DROP SCHEMA IF EXISTS IDENTIFIER('"MELS_SMOOTHIE_CHALLENGE_DB"."PUBLIC"');

//Create stage
CREATE OR REPLACE STAGE MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON
	URL = 's3://uni-lab-files-more/dlkw/trails/trails_geojson/'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 4: Data Lake Workshop'
;

CREATE OR REPLACE STAGE MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET
	URL = 's3://uni-lab-files-more/dlkw/trails/trails_parquet/'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 4: Data Lake Workshop'
;

//List stage values
list @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON;
list @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET;

/*
🎯 File Formats!
Create two files formats:

Name one FF_JSON and set the Type to JSON
Name the other FF_PARQUET and set the Type to PARQUET
Make sure they are in the TRAILS schema and are owned by SYSADMIN
We may need to add other file format properties, but for now we make the file formats as simple as possible. 
*/
--https://docs.snowflake.com/en/sql-reference/sql/create-file-format?utm_source=snowscope&utm_medium=serp&utm_term=file+format
create or replace file format MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON
type = 'JSON'
--COMPRESSION = 'AUTO'
--STRIP_OUTER_ARRAY = TRUE
COMMENT = 'Badge 4: Data Lake Workshop'
;

create or replace file format MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET
type = 'PARQUET'
--COMPRESSION = 'AUTO'
COMMENT = 'Badge 4: Data Lake Workshop'
;

//🥋 Query Your TRAILS_GEOJSON Stage!
--Try querying the TRAILS_GEOJSON stage using the very simple FF_JSON file format. If you have any issues, run a SHOW command to check for typos, ownership issues and other possible missteps. 
select $1 from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON');

//🎯 Query Your TRAILS_PARQUET Stage!
--Use the query above as an example and write a simple select statement for the data in your trails_parquet stage. 
select $1 from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET');









