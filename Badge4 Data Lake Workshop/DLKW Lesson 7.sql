//Lesson 7: Exploring GeoSpatial Functions  🥋 Explore GeoSpatial Functions 
use role sysadmin;
use warehouse compute_wh;
use schema MELS_SMOOTHIE_CHALLENGE_DB.TRAILS;
/*-----------------------------------------------------------------*/
select 
'LINESTRING('||listagg(coord_pair,',') within group (order by point_id)||')' as my_linestring 
,st_length(my_linestring) as length_of_trail
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL
group by trail_name
;
/*
001044 (42P13): SQL compilation error: error line 3 at position 1
Invalid argument types for function 'ST_LENGTH': (VARCHAR(16777216))
--
WKT Playground was nice enough to take our string (that looked like a GeoSpatial object) and convert it to a GeoSpatial Object and display it. 
But Snowflake will expect you to convert it yourself. That's easy! We can use the TO_GEOGRAPHY( ) function!
*/

/*
🎯 TO_GEOGRAPHY Challenge Lab!!
Can You Add the TO_GEOGRAPHY() Function to the query above so the length_of_trail column will work properly and no longer throw an error?

HINT: Before we can calculate the length of a LINESTRING, the data has to be a LINESTRING, not just a list of coordinates that looks like a LINESTRING (but is really just a plain, old STRING). 
*/
select 
'LINESTRING('||listagg(coord_pair,',') within group (order by point_id)||')' as my_linestring 
,st_length(TO_GEOGRAPHY(my_linestring)) as length_of_trail
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL
group by trail_name
;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL;

/*
🎯 Calculate the Lengths for the Other Trails
Use Snowflake's GeoSpatial functions to derive the length of the trails that are available in the DENVER_AREA_TRAILS view.
*/
select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS;

select
feature_name
,st_length(to_geography(geometry)) as trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
;

/*
🎯 Change your DENVER_AREA_TRAILS view to include a Length Column!
You can add a column to the view by replacing the whole view with changes.

To get a copy of a CREATE OR REPLACE VIEW code block for your existing view, run this bit of code: 

select get_ddl('view', 'DENVER_AREA_TRAILS');
Run the statement above, then copy the results (the view definition) up into the worksheet code area where you can edit it.

You can also get the data definition by navigating to the home screen, navigating to the object, and copying it from there, but the GET_DDL() function is a handy trick to know.
*/
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
COMMENT = 'Badge 4: Data Lake Workshop'
as
select 
 $1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,st_length(to_geography(geometry)) as trail_length
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON')
;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS;

//🥋 Create a View on Cherry Creek Data to Mimic the Other Trail Data
--Create a view that will have similar columns to DENVER_AREA_TRAILS
--Even though this data started out as Parquet, and we are joining it with geoJSON data
--So let's make it look like geoJSON instead
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2
COMMENT = 'Badge 4: Data Lake Workshop'
as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',')||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry)) as trail_length
from cherry_creek_trail
group by trail_name;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL;
select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2;

//🥋 Use A Union All to Bring the Rows Into a Single Result Set
--Create a view that will have similar columns to DENVER_AREA_TRAILS
select feature_name, geometry, trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2
;

//📓  Now We've Got GeoSpatial LineStrings for All 5 Trails in the Same View
--We can also compare the lengths of the various trails (listed in meters, not cheeseburgers). 
select 
 feature_name
,to_geography(geometry) as my_linestring
,trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
union all
select 
 feature_name
,to_geography(geometry) as my_linestring
,trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2
;

//🥋 But Wait! There's More!
--Add more GeoSpatial Calculations to get more GeoSpecial Information! 
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2;

//Make it a View
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES
COMMENT = 'Badge 4: Data Lake Workshop'
as
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES;

//📓  A Polygon Can be Used to Create a Bounding Box
select
 min(min_eastwest) as western_edge
,min(min_northsouth) as southern_edge
,max(max_eastwest) as eastern_edge
,max(max_northsouth) as northern_edge
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES
;

select 'POLYGON(('||
 min(min_eastwest)||' '||max(max_northsouth)||','||
 max(max_eastwest)||' '||max(max_northsouth)||','||
 max(max_eastwest)||' '||min(min_northsouth)||','||
 --min(min_eastwest)||' '||max(max_northsouth)||'))' --Not correct
 min(min_eastwest)||' '||min(min_northsouth)||'))' as my_polygon
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES
;







