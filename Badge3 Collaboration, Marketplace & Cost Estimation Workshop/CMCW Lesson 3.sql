use role sysadmin;
--use warehouse compute_wh;
use warehouse intl_wh;
use schema intl_db.public;
/*---------------------------------------------------------*/
//Lesson 3: Joining Local Data With Shared Data 


//🥋 Set Up a New Database Called INTL_DB
CREATE OR REPLACE DATABASE INTL_DB COMMENT='Badge 3: Collaboration, Marketplace & Cost Estimation Workshop';

//🥋 Create a Warehouse for Loading INTL_DB
CREATE OR REPLACE WAREHOUSE INTL_WH
WITH
WAREHOUSE_SIZE = 'XSMALL'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 60 //60 SECONDS/1 MINUTE
AUTO_RESUME = TRUE
;

USE WAREHOUSE INTL_WH;

//🥋 Create Table INT_STDS_ORG_3166
create or replace table intl_db.public.INT_STDS_ORG_3166 
(iso_country_name varchar(100), 
 country_name_official varchar(200), 
 sovreignty varchar(40), 
 alpha_code_2digit varchar(2), 
 alpha_code_3digit varchar(3), 
 numeric_country_code integer,
 iso_subdivision varchar(15), 
 internet_domain_code varchar(10)
);

//🥋 Create a File Format to Load the Table
create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR 
  type = 'CSV' --use CSV for any flat file
  compression = 'AUTO' 
  field_delimiter = '|' --pipe or vertical bar
  record_delimiter = '\r' --carriage return
  skip_header = 1  --1 header row
  field_optionally_enclosed_by = '\042'  --double quotes
  trim_space = FALSE;

//🎯 Load the ISO Table Using Your File Format
/*
Data files for this course are available from an s3 bucket named uni-cmcw.  There is only one s3 bucket in the whole world with that name and it belongs to this course. Create a new stage (you know how! you did it in Badge 1). 

Check to see if you have a stage in your account already (this will be true if you are using the same Trial Account from Badge 1). 

show stages in account; 
You can create a new stage using the wizard, or you can use the code below. 

create stage util_db.public.aws_s3_bucket url = 's3://uni-cmcw';

Make sure you create it while in the SYSADMIN role, or grant SYSADMIN rights to use the stage. 

The file you will be loading is called iso_countries_utf8_pipe.csv. BUT remember that AWS is very case sensitive, so be sure to look up the EXACT spelling of the file name for your COPY INTO statement. Remember that you can view the files in the stage either by navigating to the stage and enabling the directory table, or by running a list command like this: 

list @util_db.public.aws_s3_bucket;

And finally, here's a reminder of the syntax for COPY INTO:

copy into my_table_name
from @util_db.public.like_a_window_into_an_s3_bucket
files = ( 'IF_I_HAD_A_FILE_LIKE_THIS.txt')
file_format = ( format_name='EXAMPLE_FILEFORMAT' );
Remember that you can find the table name, file format name and stage name on this page (scroll up). 
*/

//Create stage
CREATE OR REPLACE STAGE util_db.public.aws_s3_bucket 
	URL = 's3://uni-cmcw'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 3: Collaboration, Marketplace & Cost Estimation Workshop';

//List stage values
list @util_db.public.aws_s3_bucket;

//COPY 
COPY INTO intl_db.public.INT_STDS_ORG_3166 
FROM @util_db.public.aws_s3_bucket/
FILES = ('ISO_Countries_UTF8_pipe.csv')
FILE_FORMAT = (FORMAT_NAME = 'util_db.public.PIPE_DBLQUOTE_HEADER_CR')
ON_ERROR=ABORT_STATEMENT
;
--249

//Validate
SELECT * FROM intl_db.public.INT_STDS_ORG_3166;

//🥋 Check That You Created and Loaded the Table Properly
select count(*) as found, '249' as expected from intl_db.public.INT_STDS_ORG_3166;


 //📓  How to Test Whether You Set Up Your Table in the Right Place with the Right Name
--We can "ask" the Information Schema Table called "Tables" if our table exists by asking it to count the number of times a table with that name, in a certain schema, in a certain database (catalog) exists. If it exists, we should get back the count of 1. 

select count(*) as OBJECTS_FOUND
from <database name>.INFORMATION_SCHEMA.TABLES 
where table_schema=<schema name> 
and table_name= <table name>;

--So if we are looking for INTL_DB.PUBLIC.INT_STDS_ORG_3166 we can run this command to check: 
--Does a table with that name exist...in a certain schema...within a certain database.

select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';

//📓  How to Test That You Loaded the Expected Number of Rows
--We can "ask" the Information Schema Table called "Tables" if our table has the expected number of rows with a command like this:

select row_count
from <database name>.INFORMATION_SCHEMA.TABLES 
where table_schema=<schema name> 
and table_name= <table name>;

--So if we are looking to see how many rows are contained in INTL_DB.PUBLIC.INT_STDS_ORG_3166 we can run this command to check: 
--For the table we presume exists...in a certain schema...within a certain database...how many rows does the table hold?

select row_count
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166'; 


//🥋 Join Local Data with Shared Data
select
 i.iso_country_name
,i.country_name_official
,i.alpha_code_2digit
,r.r_name as region
from intl_db.public.INT_STDS_ORG_3166 as i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION as n
on upper(i.iso_country_name) = n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION as r
on n.N_REGIONKEY = r.R_REGIONKEY
;

//🥋 Convert the Select Statement into a View
create or replace view intl_db.public.NATIONS_SAMPLE_PLUS_ISO
(iso_country_name
,country_name_official
,alpha_code_2digit
,region)
AS
select
 i.iso_country_name
,i.country_name_official
,i.alpha_code_2digit
,r.r_name as region
from intl_db.public.INT_STDS_ORG_3166 as i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION as n
on upper(i.iso_country_name) = n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION as r
on n.N_REGIONKEY = r.R_REGIONKEY
;

//🥋 Run a SELECT on the View You Created
select * from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;


//🥋 Create Table Currencies
create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
  comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

//🥋 Create a File Format to Process files with Commas, Linefeeds and a Header Row  
create file format util_db.public.CSV_COMMA_LF_HEADER
TYPE = 'CSV'--csv for comma separated files
SKIP_HEADER = 1 --one header row  
record_delimiter = '\n' -- the n represents a Line Feed character
FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
;

//List stage values
list @util_db.public.aws_s3_bucket;

//COPY 
COPY INTO intl_db.public.CURRENCIES  
FROM @util_db.public.aws_s3_bucket/
FILES = ('currencies.csv')
FILE_FORMAT = (FORMAT_NAME = 'util_db.public.CSV_COMMA_LF_HEADER')
ON_ERROR=ABORT_STATEMENT
;
--151

//Validate
select * from intl_db.public.CURRENCIES;

//🥋 Create Table Country to Currency
create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
  (
    country_char_code varchar(3), 
    country_numeric_code integer, 
    country_name varchar(100), 
    currency_name varchar(100), 
    currency_char_code varchar(3), 
    currency_numeric_code integer
  ) 
  comment = 'Mapping table currencies to countries';

//COPY 
COPY INTO intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE  
FROM @util_db.public.aws_s3_bucket/
FILES = ('country_code_to_currency_code.csv')
FILE_FORMAT = (FORMAT_NAME = 'util_db.public.CSV_COMMA_LF_HEADER')
ON_ERROR=ABORT_STATEMENT
;
--265

//🎯 Create a View that Will Return The Result Set Shown
create or replace view intl_db.public.SIMPLE_CURRENCY
as
select
 COUNTRY_CHAR_CODE as CTY_CODE
,CURRENCY_CHAR_CODE as CUR_CODE
from intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
;

//Validate
select * from intl_db.public.SIMPLE_CURRENCY;
