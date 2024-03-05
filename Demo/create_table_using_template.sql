//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
/*
creates a table with the column definitions derived from a set of staged files
*/
//CSV Files
//1.Create file format
--https://docs.snowflake.com/en/sql-reference/sql/create-file-format
CREATE OR REPLACE FILE FORMAT demo_db.public.mycsvformat
   TYPE = 'CSV'
   FIELD_DELIMITER = '|'
   PARSE_HEADER = TRUE --New Feature to support infer_schema
   ;

   
//2.Create named external stage[Snowflake S3 public Bucket]
--https://docs.snowflake.com/en/sql-reference/sql/create-stage
--External stage: References data files stored in a location outside of Snowflake
create or replace stage demo_db.public.sftutorials_ext_stg
URL = 's3://snowflake-docs'
;

--Validation
show file formats like 'mycsvformat' in schema demo_db.public;
show stages like 'sftutorials_ext_stg' in demo_db.public;
list @sftutorials_ext_stg;
list @sftutorials_ext_stg/tutorials/dataloading/;

//3.INFER_SCHEMA: Automatically detects the file metadata schema
--Table Functions: https://docs.snowflake.com/en/sql-reference/functions-table
--INFER_SCHEMA: https://docs.snowflake.com/en/sql-reference/functions/infer_schema
select * 
from table(infer_schema(location=>'@sftutorials_ext_stg/tutorials/dataloading/'
          ,files=>'contacts1.csv','contacts4.csv' --Pass one or two files 
          ,file_format=>'mycsvformat'
          ,ignore_case=>TRUE
          ));

//4.Create table 
--https://docs.snowflake.com/en/sql-reference/sql/create-table
--https://docs.snowflake.com/en/sql-reference/functions/infer_schema#examples
--OBJECT_CONSTRUCT: https://docs.snowflake.com/en/sql-reference/functions/object_construct
--ARRAY_AGG: https://docs.snowflake.com/en/sql-reference/functions/array_agg
create or replace table demo_db.public.contacts
using template(select array_agg(object_construct(*))
               from table(infer_schema(location=>'@sftutorials_ext_stg/tutorials/dataloading/'
                         ,files=>'contacts1.csv','contacts4.csv'
                         ,file_format=>'mycsvformat'
                         ,ignore_case=>TRUE))
              );
              
--Validation
show tables like 'contacts%' in schema demo_db.public;
--To get table definition
select get_ddl('table','demo_db.public.contacts');
--
create or replace TABLE CONTACTS (
	ID NUMBER(1,0),
	LASTNAME VARCHAR(16777216),
	FIRSTNAME VARCHAR(16777216),
	COMPANY VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	WORKPHONE VARCHAR(16777216),
	CELLPHONE VARCHAR(16777216),
	STREETADDRESS VARCHAR(16777216),
	CITY VARCHAR(16777216),
	POSTALCODE NUMBER(5,0)
);


//5.Copy Data Into the Target Table
--COPY INTO <table>: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
copy into demo_db.public.contacts
from '@sftutorials_ext_stg/tutorials/dataloading/'
files=('contacts1.csv')
--FILE_FORMAT = ( FORMAT_NAME = <> )
file_format = (TYPE='CSV', FIELD_DELIMITER = '|', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '\042', RECORD_DELIMITER = '\n', TRIM_SPACE = TRUE)
--VALIDATION_MODE = RETURN_5_ROWS
;

//6.Verify the Loaded Data
select * from demo_db.public.contacts;




/*------------------------------------------------------------------------------------------------*/
//JSON
--1.Create a file format that sets the file type as JSON.
create or replace file format myjsonformat
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

list @sftutorials_ext_stg;

--Query external stage files directly
--https://docs.snowflake.com/en/user-guide/querying-stage
select parse_json($1) FROM @sftutorials_ext_stg/tutorials/dataloading/contacts.json (file_format => 'myjsonformat');

--2.Query the INFER_SCHEMA function.
select * 
from table(infer_schema(location=>'@sftutorials_ext_stg/tutorials/dataloading/'
          ,files=>'contacts.json'
          ,file_format=>'myjsonformat'
          ,ignore_case=>TRUE
          ));

--3.Create table using template
create or replace table demo_db.public.contacts_json
using template(select array_agg(object_construct(*))
               from table(infer_schema(location=>'@sftutorials_ext_stg/tutorials/dataloading/'
                         ,files=>'contacts.json'
                         ,file_format=>'myjsonformat'
                         ,ignore_case=>TRUE))
              );

--Validation
show tables like 'contacts%' in schema demo_db.public;
select get_ddl('table','demo_db.public.contacts_json');
create or replace TABLE CONTACTS_JSON (
	CUSTOMER OBJECT
);

//4.Copy Data Into the Target Table
copy into demo_db.public.contacts_json
from '@sftutorials_ext_stg/tutorials/dataloading/'
files=('contacts.json')
file_format = 'myjsonformat'
--VALIDATION_MODE = RETURN_5_ROWS
;

//5.Verify the Loaded Data
select * from demo_db.public.contacts_json;

--Parse JSON into relational form
select 
 lf.VALUE:_id::string as id
,lf.VALUE:address::string as address
,lf.VALUE:company::string as company
,lf.VALUE:email::string as email
,lf.VALUE:name.first::string as first_name
,lf.VALUE:name.last::string as last_name
,lf.VALUE:phone::string as phone
from demo_db.public.contacts_json as t1
,lateral flatten(input => t1.customer) as lf
;



/*------------------------------------------------------------------------------------------------*/
//PARQUET

--1.Create a file format that sets the file type as Parquet.
CREATE FILE FORMAT myparquetformat
  TYPE = parquet;

list @sftutorials_ext_stg;

--2.Query the INFER_SCHEMA function.
select * 
from table(infer_schema(location=>'@sftutorials_ext_stg/tutorials/dataloading/cities.parquet'
          ,file_format=>'myparquetformat'
          ,ignore_case=>TRUE
          ));

--3.Create table using template
create or replace table demo_db.public.cities
using template(select array_agg(object_construct(*))
               from table(infer_schema(location=>'@sftutorials_ext_stg/tutorials/dataloading/cities.parquet'
                         ,file_format=>'myparquetformat'
                         ,ignore_case=>TRUE))
              );

--Validation
show tables like 'cities%' in schema demo_db.public;
select get_ddl('table','demo_db.public.cities');
create or replace TABLE CITIES (
	CONTINENT VARCHAR(16777216),
	COUNTRY VARIANT
);

--Transforming Data During a Load
--https://docs.snowflake.com/en/user-guide/data-load-transform
select 
 $1
from '@sftutorials_ext_stg/tutorials/dataloading/cities.parquet'
(file_format => 'myparquetformat')
;

//4.Copy Data Into the Target Table
copy into demo_db.public.cities
from (select 
 $1:continent::string as continent
,$1:country::variant as country
from '@sftutorials_ext_stg/tutorials/dataloading/cities.parquet'
(file_format => 'myparquetformat')
)
;

//5.Verify the Loaded Data
select * from demo_db.public.cities;

select
 continent
,country:name::string as country_name  
,country:city::array as city
from demo_db.public.cities
;