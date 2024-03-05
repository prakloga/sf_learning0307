//Lesson 6: GeoSpatial Views  🥋 Querying the Parquet File 
use role sysadmin;
use warehouse compute_wh;
use schema MELS_SMOOTHIE_CHALLENGE_DB.TRAILS;
/*-----------------------------------------------------------------*/
//🥋 Look at the Parquet Data
--Run a select, then click on any row to see it's data. Is this data nested?
select $1 from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET');

--Write a more sophisticated query to parse the data into columns. We give you the first two rows. We know you can figure out the rest.
select
 $1:sequence_1 as sequence_1
,$1:trail_name::string as trail_name
,$1:latitude as latitude
,$1:longitude as longitude
,$1:sequence_2 as sequence_2
,$1:elevation as elevation
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET')
;

// 🥋 Use a Select Statement to Fix Some Issues
/*
According to some online blog posts, you don't need more than 8 decimal points on coordinates to get accuracy to within a millimeter. Remember that Latitudes are between 0 (the equator)  and 90 (the poles) so no more than 2 digits are needed left of the decimal for latitude data.
Longitudes are between 0 (the prime meridian) and 180. So no more than 3 digits are needed to the left of the decimal for longitude data.
If we cast both longitude and latitude data as NUMBER(11,8) we should be safe.  We have included the code for this select statement below.
*/
select
 $1:sequence_1 as sequence_1
,$1:trail_name::string as trail_name
,$1:latitude::number(11,8) as lng
,$1:longitude::number(11,8) as lat
--,$1:sequence_2 as sequence_2
--,$1:elevation as elevation
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET')
;

--Nicely formatted trail data
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET')
order by point_id;
/*
After running this select statement, you can copy and paste one set of coordinates into the WKT Playground site to see if it looks accurate. Snowflake has functions for working with geometry and geography data, but no way to overlay it on maps, yet. 
*/

//🎯 Create a View Called CHERRY_CREEK_TRAIL
/*
Wrap the select statement in a CREATE VIEW.
Name it CHERRY_CREEK_TRAIL. 
Make sure it is in Mel's database, in his TRAILS schema.
Make sure it is owned by SYSADMIN.
*/
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL
COMMENT = 'Badge 4: Data Lake Workshop'
as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET')
order by point_id
;

//🥋 Use || to Chain Lat and Lng Together into Coordinate Sets!
--Now we can make pairs with a space in between, since we know that's how WKT Playground likes them formatted!
select
 lng||' '||lat as coord_pair
,'POINT('||coord_pair||')' as trail_point
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL
limit 100
;

--Using concatenate to prepare the data for plotting on a map
select top 100 
 lng||' '||lat as coord_pair
,'POINT('||coord_pair||')' as trail_point
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL
;

--To add a column, we have to replace the entire view
--changes to the original are shown in red
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL
COMMENT = 'Badge 4: Data Lake Workshop'
as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET')
order by point_id
;

//Validation
select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL limit 100;

/*
🥋 Let's Collapse Sets Of Coordinates into Linestrings! 
We can use Snowflakes LISTAGG function and the new COORD_PAIR column to make LINESTRINGS we can paste into WKT Playground! 

Let's remember the syntax for LINESTRINGS. 

LINESTRING(
Coordinate Pair
COMMA
Coordinate Pair
COMMA
Coordinate Pair
(etc)
) 
*/
select listagg(coord_pair,',') within group (order by point_id) from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL;
select 'LINESTRING('||listagg(coord_pair,',') within group (order by point_id)||')' as my_linestring 
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL;

//🥋 Look at the geoJSON Data
--Run a select on the geoJSON Stage, using the JSON file format you created. If you can't remember their names, just use SHOW commands to remind yourself. 
select $1 from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON');

//🥋 Normalize the Data Without Loading It!
select 
 $1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON')
;

// 🥋 Visually Display the geoJSON Data
--Again, we can manage and massage the data in Snowflake, but we can't really display it properly. So just as with the WKT formatted GeoSpatial data, we need another tool to visually display the data we store in Snowflake. For this exploration we'll go to geojson.io.

//🎯 Create a View Called DENVER_AREA_TRAILS
/*
Wrap the previous select statement in a CREATE VIEW statement.
Name it DENVER_AREA_TRAILS. 
Make sure it is in Mel's database, in his TRAILS schema.
Make sure it is owned by SYSADMIN. 
*/
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
COMMENT = 'Badge 4: Data Lake Workshop'
as
select 
 $1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON (file_format => 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON')
;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS;






















