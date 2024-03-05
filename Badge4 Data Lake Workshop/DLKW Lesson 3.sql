use role sysadmin;
use warehouse compute_wh;
use schema "ZENAS_ATHLEISURE_DB"."PRODUCTS";
/*-------------------------------------------------------------------*/
//Lesson 3: Leaving the Data Where it Lands | 🥋 Leaving Data Where It Lands!

//🥋 List Commands Versus Select Statements 
--Run a list command on the @UNI_KLAUS_ZMD Stage. 
--How many columns are there, and how many rows (files)?  
//List stage values
list @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING;
list @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD;
list @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_SNEAKERS;

/*
📓 Simple Selects on Non-Loaded Data
We've done this before in other workshops. Remember that we can query data in a file, before we even load it. Let's explore the 3 flat files in the ZMD stage. 
We're going to use a select statement on files in the @UNI_KLAUS_ZMD Stage, which we'll refer to as "the ZMD" for short. We can't run a select star - it won't work for data that hasn't been loaded.
Since we know very little about the structure of the files, let's just see what appears in the first column ($1) of each file. 
*/
//🥋 Query Data in the ZMD 
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD;

//📓 One File at a Time?
//🥋 Query Data in Just One File at a Time 
--Can you modify the select statement so it only queries one file at a time? Pick a file name from the LIST command, and try to change your SELECT statement so it only queries the data from one of the 3 files.
--Try it, and then go to the next page to see how we wrote our query. 
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/product_coordination_suggestions.txt;
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/sweatsuit_sizes.txt;
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/swt_product_line.txt;

//📓 What is Going On Here?
/*
The data looks really weird, right? 

Snowflake hasn't been told anything about how the data in these files is structured so it's just making assumptions.  Snowflake is presuming that the files are CSVs because CSVs are a very popular file-formatting choice. It's also presuming each row ends with CRLF (Carriage Return Line Feed) because CRLF is also very common as a row delimiter.

Snowflake hedges its bets and presumes if you don't tell it anything about your file, the file is probably a standard CSV.

By using these assumptions, Snowflake treats the product_coordination_suggestions.txt file as if it only has one column and one row. 


📓 How Can We Tell Snowflake More about the Structure of Our File?
Of course you know the answer because we've been using File Formats since Badge 1.

We need to create some File Formats to help guide Snowflake in handling these files.
*/

//🥋 Create an Exploratory File Format
--Let's create a file format to test whether the carets are supposed to separate one row from another.
//🥋 Create a File Format to Load the Table
create or replace file format ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_1 
record_delimiter = '^'
;

//🥋 Use the Exploratory File Format in a Query
--Run the SELECT using the FILE FORMAT we created. See if you think the data seems more clear. 
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/product_coordination_suggestions.txt (file_format => 'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_1');

//📓 An Alternate Theory
--What if the carets aren't the row separators? What if they are the column separators, instead?
--Let's create a second exploratory file format, and see what things look like when we use that one. 
create or replace file format ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_2 
field_delimiter = '^'
;

select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/product_coordination_suggestions.txt (file_format => 'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_2');

//🥋 A Third Possibility?
/*What if the carets separate records and a different symbol is used to separate the columns? Can you write a new File Format  (call it zmd_file_format_3) to make the results look like this? 
You'll need to define both the field delimiter and the row delimiter to make it work. Be sure to replace the question marks with the real delimiters!
*/
create or replace file format ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_3 
field_delimiter = '='
record_delimiter = '^'
trim_space = TRUE
;

select $1, $2 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/product_coordination_suggestions.txt (file_format => 'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_3');

//🎯 Revise zmd_file_format_1
/*
Let's repurpose file format 1 so it can be used to parse another file in the ZMD stage! 

Here's your challenge lab task!

Rewrite zmd_file_format_1 to parse sweatsuit_sizes.txt
You can either DROP the old file format and create a new one with the same name, or you can add the phrase "OR REPLACE" to the "CREATE FILE FORMAT" statement.

Once you've replaced zmd_file_format_1, use it to query the sweatsuit_sizes.txt file. 
*/
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/sweatsuit_sizes.txt;

create or replace file format ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_1 
record_delimiter = ';'
trim_space = TRUE
;

select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/sweatsuit_sizes.txt (file_format =>'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_1');
/*
There's some weird spacing on many of the rows. Rows 7 and 13 are the worst. 
Don't worry about the spacing right now. We'll come back and fix it later. 
Also, if you end up with an empty row at the end (for a total of 19 rows) don't worry about that right now. We'll deal with that problem in the next few pages. 
*/

//📓 Another, More Useful, File Format
--Let's repurpose file format 2 so it can be used to parse the swt_product_line file.  What delimiters do you think might be used in this file?  
--What's the record delimiter and what's the field delimiter? 
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/swt_product_line.txt;

create or replace file format ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_2 
field_delimiter = '|'
record_delimiter = ';'
trim_space = TRUE
;

select $1, $2, $3 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/swt_product_line.txt (file_format => 'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_2');

//🥋 One More Thing!
--After you update zmd_file_format_2 to parse swt_product_line.txt let's fix some of the weird formatting issues in some of the columns.
--Add the TRIM_SPACE property to the file format. Set the property to TRUE and re-run the SELECT. Did that fix some of the issues? 
select $1, $2, $3 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/swt_product_line.txt (file_format => 'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_2');

//🥋 Dealing with Unexpected Characters
/*
Many data files use CRLF (Carriage Return Line Feed) as the record delimiter, so if a different record delimiter is used, the CRLF can end up displayed or loaded! When strange characters appear in your data, you can refine your select statement to deal with them. 

In SQL we can use ASCII references to deal with these characters. 

13 is the ASCII for Carriage return
10 is the ASCII for Line Feed
SQL has a function, CHR() that will allow you to reference ASCII characters by their numbers.  So, chr(13) is the same as the Carriage Return character and chr(10) is the same as the Line Feed character. 

In Snowflake, we can CONCATENATE two values by putting || between them (a double pipe). So we can look for CRLF by telling Snowflake to look for:

 chr(13)||chr(10)
*/
--                 Order matters
select replace($1, chr(13)||chr(10)) as sizes_available
from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/sweatsuit_sizes.txt (file_format =>'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_1');

//📓 Uh Oh - One more CRLF Issue!
--Did you see Row 19? It's caused by an extra CRLF at the end of the file. Add a WHERE clause to your SELECT to nix that row!
select replace($1, chr(13)||chr(10)) as sizes_available
from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/sweatsuit_sizes.txt (file_format =>'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_1')
where sizes_available <> '';

//🥋 Convert Your Select to a View
--Add this line above your select statement, to convert the SELECT statement to a view.
create or replace view ZENAS_ATHLEISURE_DB.PRODUCTS.sweatsuit_sizes
COMMENT = 'Badge 4: Data Lake Workshop'
as
select replace($1, chr(13)||chr(10)) as sizes_available
from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/sweatsuit_sizes.txt (file_format =>'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_1')
where sizes_available <> '';

select * from ZENAS_ATHLEISURE_DB.PRODUCTS.sweatsuit_sizes;



//🎯 Make the Sweatband Product Line File Look Great!
/*
REPLACE file format 2 so that the DELIMITERS are correct to process the sweatband data file. 
Remove leading spaces in the data with the TRIM_SPACE property. 
Remove CRLFs from the data (via your select statement).
If there are any weird, empty rows, remove them (also via the select statement).
Put a view on top of it to make it easy to query in the future! Name your view:  zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE
Don't forget to NAME the columns in your Create View statement. You can see the names you should use for your columns in the screenshot. 
*/
create or replace view ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_PRODUCT_LINE
COMMENT = 'Badge 4: Data Lake Workshop'
as
select replace($1, chr(13)||chr(10)) as product_code
, replace($2, chr(13)||chr(10)) as headband_description
, replace($3, chr(13)||chr(10)) as wristband_description
from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/swt_product_line.txt (file_format => 'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_2');

select * from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_PRODUCT_LINE;



//🎯 Make the Product Coordination Data Look great!
/*
File format 3 is already working for the product coordination data set, since it doesn't have a lot going on. 
Remove CRLFs from the data (via your select statement).
If there are any weird, empty rows, remove them (also via the select statement).
Put a view on top of it to make it easy to query in the future! Name your view:  zenas_athleisure_db.products.SWEATBAND_COORDINATION
Give your view columns nice names!  (see screenshot)
*/
create or replace view ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_COORDINATION
COMMENT = 'Badge 4: Data Lake Workshop'
as
select replace($1, chr(13)||chr(10)) as product_code
, replace($2, chr(13)||chr(10)) as has_matching_sweatsuit
from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD/product_coordination_suggestions.txt (file_format => 'ZENAS_ATHLEISURE_DB.PRODUCTS.zmd_file_format_3')
;

select * from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_COORDINATION;












