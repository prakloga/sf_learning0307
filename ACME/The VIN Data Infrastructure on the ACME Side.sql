/* 📓 The VIN Data Infrastructure on the ACME Side
Caden will build out the VIN infrastructure she needs in the ACME Account. 

🎯 Create New Objects in ACME
Create a new database in the ACME Account called STOCK. 
Drop the PUBLIC schema. 
Add a new schema named UNSOLD. 
Make sure both the database and new schema are owned by SYSADMIN.
*/
CREATE DATABASE IF NOT EXISTS IDENTIFIER('"STOCK"') COMMENT = '';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('"STOCK"."UNSOLD"') COMMENT = '';
DROP SCHEMA IF EXISTS  IDENTIFIER('"STOCK"."PUBLIC"');

//🥋 ACME's Lot Stock Table
--Caden needs a table that stores the ACME Car Inventory
create or replace table stock.unsold.lotstock
(
  vin varchar(25)
, exterior varchar(50)	
, interior varchar(50)
, manuf_name varchar(25)
, vehicle_type varchar(25)
, make_name varchar(25)
, plant_name varchar(25)
, model_year varchar(25)
, model_name varchar(25)
, desc1 varchar(25)
, desc2 varchar(25)
, desc3 varchar(25)
, desc4 varchar(25)
, desc5 varchar(25)
, engine varchar(25)
, drive_type varchar(25)
, transmission varchar(25)
, mpg varchar(25)
);

/*
🎯 Look at the File ACME Receives from their Vehicle Delivery Company
The file is located in a stage. The same stage you used to load Max's file. 

The problem is that you haven't created a stage on the ACME account, yet.

You need a Stage in ACME that points to the same S3 bucket as the stage you created  in the ADU account. 

Create a stage named aws_s3_bucket in the STOCK.UNSOLD schema of the ACME account. 

(We believe in you!) 

Once you have the stage created, find a file that you think might be Lottie's data for the LotStock table. 
*/


//🎯 Create an AWS External Stage
/*Even though our ADU Account is on GCP, we can still pull files from an AWS Stage. Storage from all 3 cloud providers work seamlessly with Snowflake accounts on any other provider.

It should be an External AWS Stage
The URL is  s3://uni-cmcw/  
The stage should be in the VIN.DECODE schema. 
You can name it whatever you want, but aws_s3_bucket might be easiest.
It should be owned by SYSADMIN
*/
//Create stage
CREATE OR REPLACE STAGE stock.unsold.aws_s3_bucket 
	URL = 's3://uni-cmcw/'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 3: Collaboration, Marketplace & Cost Estimation Workshop';

//List stage values
list @stock.unsold.aws_s3_bucket;

//🥋 Query the File Before Loading It
-- Fill in the rest of the file name by looking at the files in your new stage
-- Replace the question marks with the file name (remember AWS is case sensitive)
select $1, $2, $3
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv
;

//📓 Telling Snowflake More About Our Not-Yet-Loaded File
--When querying a file from a stage, Snowflake presumes data is comma-separated (unless you tell it something different) but we can make the data look even better if we query it with the help of a file format. 

//🥋 A File Format to Help Max Load the Data
--Create a file format and then load each of the 5 Lookup Tables
--You need a file format if you want to load the table
CREATE FILE FORMAT UTIL_DB.PUBLIC.CSV_COMMA_LF_HEADER
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1 
field_optionally_enclosed_by = '"'  
trim_space = TRUE;

//🥋 Query the File Again, with the Help of a File Format
-- Replace the question marks with the file name (remember AWS is case sensitive)
-- Notice that we use AS to rename the columns and we are now using a file format 
-- The file format knows to skip the first row because it is a header row
select 
 $1 as VIN
,$2 as Exterior
,$3 as Interior
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv
(file_format => 'UTIL_DB.PUBLIC.CSV_COMMA_LF_HEADER')
;

/*
📓 How to Load a File of 3 Columns into a Table of 18 Columns?
Did you notice that the file we are loading only has 3 columns or fields? The VIN, the Exterior Colors and the Interior Colors are the columns in our file. The other columns in the table will be empty until we use Max's DECODE functionality to populate them. 

To load our file into our table, we'll need a new File Format with two new properties. 

We'll replace the SKIP_HEADER property with a PARSE_HEADER property. This will tell Snowflake to look at that first row and use it to figure out the column names. 

We'll also add a ERROR_ON_COLUMN_COUNT_MISMATCH property. By setting this property FALSE, we'll be telling Snowflake that it's fine if the file has 3 columns but the table has 18. 
*/

//🥋 Another File Format
-- This file format will allow the 3 column file to be loaded into an 18 column table
-- By parsing the header, Snowflake can infer the column names
create or replace file format util_db.public.CSV_COL_COUNT_DIFF
type = 'CSV'
field_delimiter = ','
record_delimiter = '\n'
field_optionally_enclosed_by = '"'
trim_space = TRUE
error_on_column_count_mismatch = FALSE //KEY SETTING
parse_header = TRUE //KEY SETTING
;

//🥋 A Special COPY INTO
-- With a parsed header, Snowflake can MATCH BY COLUMN NAME during the COPY INTO
COPY INTO stock.unsold.lotstock
FROM @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv
FILE_FORMAT = (FORMAT_NAME = 'util_db.public.CSV_COL_COUNT_DIFF')
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
;
--300

//📓 View the Table and Its Conents
--After running the statement above, run a SELECT * on the LOTSTOCK table. How does the data look?
--The first 3 columns probably look great. But the other columns are all empty, right? We sure could use some help from Max!
SELECT * FROM stock.unsold.lotstock;

//🎯 "Get" the New Share
/*Check to see if the Listing Max sent ACME has finished replicating. 
If it has, GET it -- and be sure to include access for the SYSADMIN Role.
Name the new "database" ADU_VIN
We didn't include any sample queries, so just click [Done]*/

//🎯 Browse The Share
/*How many tables are included in the share from Max? 0
How many views? 0
How many functions? 1
Can you (as Lottie or Caden using the ACME Account) view the logic being used by the function? No
*/

//🥋 Run the UDTF
--If ACME Can't see the tables and their data, how can they run the function that uses those tables and their data!
select * from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN('5UXCR6C0XL9C77256'));

//🥋 Combining the Table Data with the Function Data
--A simple select from Lot Stock (choose any VIN from the LotStock table)
select * from STOCK.UNSOLD.LOTSTOCK where VIN = '5J8YD4H86LL013641';

select
 ls.vin
,ls.exterior
,ls.interior
,pf.*
from (select * from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN('5UXCR6C0XL9C77256'))) as pf
join STOCK.UNSOLD.LOTSTOCK as ls
on pf.VIN = ls.VIN
;

//🥋 Use a Variable Instead
--we can use a local (session) variable to make it easier to change the VIN we are trying to enhance
select * from STOCK.UNSOLD.LOTSTOCK limit 100;

set my_vin = 'SADCP2FX2LA617413';
select $my_vin;

select
 ls.VIN
,pf.MANUF_NAME
,pf.VEHICLE_TYPE
,pf.MAKE_NAME
,pf.PLANT_NAME
,pf.MODEL_YEAR
,pf.MODEL_NAME
,pf.DESC1
,pf.DESC2
,pf.DESC3
,pf.DESC4
,pf.DESC5
,pf.ENGINE
,pf.DRIVE_TYPE
,pf.TRANSMISSION
,pf.MPG
from STOCK.UNSOLD.LOTSTOCK as ls
join (select VIN,	MANUF_NAME,	VEHICLE_TYPE,	MAKE_NAME,	PLANT_NAME,	MODEL_YEAR,	MODEL_NAME,	DESC1,	DESC2,	DESC3,	DESC4,	DESC5,	ENGINE,	DRIVE_TYPE,	TRANSMISSION,MPG 
from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN($my_vin))) as pf
on pf.VIN = ls.VIN
;

//🥋 Don't Just Select It, Store It!
-- We're using "s" for "source." The joined data from the LotStock table and the parsing function will be a source of data for us. 
-- We're using "t" for "target." The LotStock table is the target table we want to update.

update STOCK.UNSOLD.LOTSTOCK as t
set manuf_name = s.manuf_name
, vehicle_type = s.vehicle_type
, make_name = s.make_name
, plant_name = s.plant_name
, model_year = s.model_year
, desc1 = s.desc1
, desc2 = s.desc2
, desc3 = s.desc3
, desc4 = s.desc4
, desc5 = s.desc5
, engine = s.engine
, drive_type = s.drive_type
, transmission = s.transmission
, mpg = s.mpg
from (select
 ls.VIN
,pf.MANUF_NAME
,pf.VEHICLE_TYPE
,pf.MAKE_NAME
,pf.PLANT_NAME
,pf.MODEL_YEAR
,pf.MODEL_NAME
,pf.DESC1
,pf.DESC2
,pf.DESC3
,pf.DESC4
,pf.DESC5
,pf.ENGINE
,pf.DRIVE_TYPE
,pf.TRANSMISSION
,pf.MPG
from STOCK.UNSOLD.LOTSTOCK as ls
join (select VIN,	MANUF_NAME,	VEHICLE_TYPE,	MAKE_NAME,	PLANT_NAME,	MODEL_YEAR,	MODEL_NAME,	DESC1,	DESC2,	DESC3,	DESC4,	DESC5,	ENGINE,	DRIVE_TYPE,	TRANSMISSION,MPG 
from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN($my_vin))) as pf
on pf.VIN = ls.VIN
)s
where s.VIN = t.VIN
;

//Validate
select * from STOCK.UNSOLD.LOTSTOCK where VIN in ('5J8YD4H86LL013641','SAJAJ4FX8LCP55916','SADCP2FX2LA617413');

//🎯 View Your Updates and Update a Few Other Rows 
/*
Check the LotStock table, did your update statement work? 
Try setting the $my_vin value to a different value (any value from the LotStock table) and see if you can update that row as well.
Try to fill in at least 3 rows of your table using the code above. 
*/

//🥋 Setting a Variable with a SQL Query
-- We can count the number of rows in the LotStock table that have not yet been updated.  
set row_count = (select count(*) 
                from stock.unsold.lotstock
                where manuf_name is null);

select $row_count;

//📓 A Look at a SQL Scripting Block
/*
Words like DECLARE, BEGIN, END, FOR are for "control of flow". They allow you to dictate which statements will take place in a certain order and be run one after another. 
The code block below will allow you to update all the remaining LotStock rows by clicking RUN just one more time.
Before running the code block below, look at the images below to see what each part is doing. 
*/
-- This scripting block runs very slow, but it shows how blocks work for people who are new to using them
DECLARE
    update_stmt varchar(2000);
    res RESULTSET;
    cur CURSOR FOR select vin from stock.unsold.lotstock where manuf_name is null;
BEGIN
    OPEN cur;
    FOR each_row IN cur DO
        update_stmt := 'update stock.unsold.lotstock t '||
            'set manuf_name = s.manuf_name ' ||
            ', vehicle_type = s.vehicle_type ' ||
            ', make_name = s.make_name ' ||
            ', plant_name = s.plant_name ' ||
            ', model_year = s.model_year ' ||
            ', desc1 = s.desc1 ' ||
            ', desc2 = s.desc2 ' ||
            ', desc3 = s.desc3 ' ||
            ', desc4 = s.desc4 ' ||
            ', desc5 = s.desc5 ' ||
            ', engine = s.engine ' ||
            ', drive_type = s.drive_type ' ||
            ', transmission = s.transmission ' ||
            ', mpg = s.mpg ' ||
            'from ' ||
            '(       select ls.vin, pf.manuf_name, pf.vehicle_type ' ||
                    ', pf.make_name, pf.plant_name, pf.model_year ' ||
                    ', pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5 ' ||
                    ', pf.engine, pf.drive_type, pf.transmission, pf.mpg ' ||
                'from stock.unsold.lotstock ls ' ||
                'join ' ||
                '(   select' || 
                '     vin, manuf_name, vehicle_type' ||
                '    , make_name, plant_name, model_year ' ||
                '    , desc1, desc2, desc3, desc4, desc5 ' ||
                '    , engine, drive_type, transmission, mpg ' ||
                '    from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN(\'' ||
                  each_row.vin || '\')) ' ||
                ') pf ' ||
                'on pf.vin = ls.vin ' ||
            ') s ' ||
            'where t.vin = s.vin;';
        res := (EXECUTE IMMEDIATE :update_stmt);
    END FOR;
    CLOSE cur;   
END;


//Validation
select * from stock.unsold.lotstock;
select * from stock.unsold.lotstock where manuf_name is null;

//📓  What's Next? 
/*
You can find more information on scripting here:  https://docs.snowflake.com/en/sql-reference-snowflake-scripting
Most people put scripting blocks into Stored Procedures, which are another way to encapsulate different bits of code. 
Snowflake was designed for loading and updating large record sets with a single statement, not for updating one row at a time, using a FOR LOOP. 
There are more efficient ways to achieve the result we achieved above, but this lesson's example allowed you to see how each part became a building block for the next. 
*/

//🎯 Navigate to Your Usage Page Yet Again!
--Looking at all our accounts over the entire course of the workshop, we found we used about 9 Credits of Compute and all costs, for all 4 accounts, were less than $30.

select *
   from STOCK.UNSOLD.LOTSTOCK
   where engine like '%.5 L%'
   or plant_name like '%z, Sty%'
   or desc2 like '%xDr%'
;







