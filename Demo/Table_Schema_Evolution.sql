//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
//Table Schema Evolution: Semi-structured data tends to evolve over time. Systems that generate data add new columns to accommodate additional information, which requires downstream tables to evolve accordingly.
--https://docs.snowflake.com/en/user-guide/data-load-schema-evolution

//Step: 1
--file1_0_0_0.parquet: has 3 columns 2 records

//Create named internal stage
create stage if not exists sf_int_stg
COPY_OPTIONS = (ON_ERROR='skip_file')
;

--Validation
show stages in database demo_db;
list @sf_int_stg;

--Create PARQUET file format
create file format if not exists demo_db.public.myparquetformat
type = parquet
;

--validation
show file formats in database demo_db;

-- Create table, with the column definitions derived from the staged sf_int_stg/file1_0_0_0.parquet file.
CREATE OR REPLACE TABLE demo_db.public.table_schema_evolution
  USING TEMPLATE (
    SELECT ARRAY_AGG(object_construct(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@sf_int_stg/file1_0_0_0.parquet',
          FILE_FORMAT=>'myparquetformat'
          --,ignore_case=>TRUE
        )
      ));

--Validation
show tables like 'table_schema_evolution' in schema demo_db.public;
select get_ddl('table','demo_db.public.table_schema_evolution');
describe table demo_db.public.table_schema_evolution;
/*
create or replace TABLE TABLE_SCHEMA_EVOLUTION (
	_COL_0 VARCHAR(16777216),
	_COL_1 VARCHAR(16777216),
	_COL_2 VARCHAR(16777216)
);
*/

-- Use the SECURITYADMIN role or another role that has the global MANAGE GRANTS privilege.
-- Grant the EVOLVE SCHEMA privilege to any other roles that could insert data and evolve table schema in addition to the table owner.
--use role securityadmin;
--GRANT EVOLVE SCHEMA ON TABLE demo_db.public.table_schema_evolution TO ROLE sysadmin;

//Enable schema evolution on the table.
-- https://docs.snowflake.com/en/sql-reference/sql/create-table
-- https://docs.snowflake.com/en/sql-reference/sql/alter-table
alter table demo_db.public.table_schema_evolution set ENABLE_SCHEMA_EVOLUTION = TRUE;

--Validation
show tables like 'table_schema_evolution' in schema demo_db.public;

-- Load a new set of data into the table.
copy into demo_db.public.table_schema_evolution
from '@sf_int_stg/file1_0_0_0.parquet' --has only three column
file_format = (type=parquet)
match_by_column_name = case_insensitive --important option
;

--Validation
describe table demo_db.public.table_schema_evolution;
select * from demo_db.public.table_schema_evolution;




//Step: 2
--file2_0_0_0.parquet: has 4 columns & 5 records
-- Load a new set of data into the table.
copy into demo_db.public.table_schema_evolution
from '@sf_int_stg/file2_0_0_0.parquet' -- has 4 columns
file_format = (type=parquet)
match_by_column_name = case_insensitive --import option
;

--Validation
describe table demo_db.public.table_schema_evolution;
select * from demo_db.public.table_schema_evolution;

















































//Prerequisite
/*------------------------------------------------------------------------------------*/
create or replace table demo_db.public.sample_data_table
(col1 varchar
,col2 varchar
,col3 varchar
,col4 varchar
)
comment = 'Sample data table'
;

insert into demo_db.public.sample_data_table values('Gannon Roach','Kayseri','Brazil','nibh.aliquam@icloud.couk')
,('Adrienne Carroll','Derbyshire','Indonesia','diam.nunc@hotmail.couk')
,('Len Logan','Nunavut','Colombia','mattis.cras.eget@protonmail.edu')
,('Haley Huber','Dadra and Nagar Haveli','India','quis@icloud.couk')
,('Gabriel Price','Centre','United States','arcu.sed@icloud.edu')
;

select * from demo_db.public.sample_data_table;

//Create named internal stage
CREATE OR REPLACE STAGE sf_int_stg
COPY_OPTIONS = (ON_ERROR='skip_file')
;

--Validation
show stages in database demo_db;
list @sf_int_stg;
--rm @sf_int_stg/unload/file1_0_0_0.parquet;


//Unload table data into internal stage
COPY INTO @sf_int_stg/file2 from (select col1, col2, col3, col4 from demo_db.public.sample_data_table)
FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = NONE);