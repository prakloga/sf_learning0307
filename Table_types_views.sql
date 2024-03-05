use role sysadmin;
use warehouse compute_wh;
use schema demo_db.public;

create or replace temporary table mytemptable(id number, creation_date date);
show tables like 'mytemptable';

create or replace transient table mytrantable(id number, creation_date date);
show tables like 'mytrantable';

create or replace table hospital_table(patient_id INTEGER,
                             patient_name VARCHAR, 
                             billing_address VARCHAR,
                             diagnosis VARCHAR, 
                             treatment VARCHAR,
                             cost NUMBER(10,2));

insert into hospital_table (patient_ID, patient_name, billing_address, diagnosis, treatment, cost) 
    VALUES
        (1, 'Mark Knopfler', '1982 Telegraph Road', 'Industrial Disease', 
            'a week of peace and quiet', 2000.00),
        (2, 'Guido van Rossum', '37 Florida St.', 'python bite', 'anti-venom', 
            70000.00)
        ;

select * from hospital_table;

create or replace view doctor_view
as
select patient_id, patient_name, diagnosis, treatment from hospital_table;

create or replace view accountant_view
as
select patient_id, patient_name, billing_address, cost from hospital_table;

//Show all of the types of medical problems for each patient:
select distinct diagnosis from doctor_view;

//Show cost of the treatment
select
 dv.patient_id
,dv.patient_name
,av.cost
from doctor_view as dv
join accountant_view as av
on dv.patient_id = av.patient_id
;

select * from demo_db.information_schema.views;


CREATE TABLE widgets (
    id NUMBER(38,0) AUTOINCREMENT, 
    name VARCHAR,
    color VARCHAR,
    price NUMBER(38,0),
    created_on TIMESTAMP_LTZ(9));

--Warehouse 'COMPUTE_WH' cannot be resumed because resource monitor 'DAILY_SHUTDOWN' has exceeded its quota.
ALTER RESOURCE MONITOR IDENTIFIER('"DAILY_SHUTDOWN"') 
SET CREDIT_QUOTA = 3 
TRIGGERS ON 75 PERCENT DO SUSPEND 
         ON 98 PERCENT DO SUSPEND_IMMEDIATE 
         ON 50 PERCENT DO NOTIFY
;
ALTER ACCOUNT SET RESOURCE_MONITOR = 'DAILY_SHUTDOWN';

insert into widgets (name, color, price, created_on)
values('Small round widget', 'Red', 1, current_timestamp());

insert into widgets (name, color, price, created_on)
values('Small cylinder widget', 'Blue', 2, current_timestamp());

insert into widgets (name, color, price, created_on)
values('Small square widget', 'Purple', 3, current_timestamp());

select * from widgets;

select * from widgets where 1/iff(color = 'Yellow',0,1) = 1;

CREATE TABLE inventory (product_ID INTEGER, wholesale_price FLOAT, description VARCHAR);
create or replace materialized view mv1 as select product_ID, wholesale_price from inventory;
insert into inventory(product_ID, wholesale_price, description)
values(1,1.00,'cog');
select * from inventory;
select product_ID, wholesale_price from inventory;
select product_ID, wholesale_price from mv1;

CREATE or replace  TABLE sales (product_ID INTEGER, quantity INTEGER, price FLOAT);

INSERT INTO sales (product_ID, quantity, price) VALUES 
   (1,  1, 1.99);
   
CREATE or replace VIEW profits AS
select
 m.product_id
,sum(ifnull(s.quantity,0)) as quantity
,sum(ifnull(quantity * (s.price - m.wholesale_price),0)) as profit
from mv1 as m
left outer join sales as s
on m.product_ID = s.product_ID
group by m.product_id
;

CREATE or replace VIEW profits AS
select
 m.product_id
,sum(ifnull(s.quantity,0)) as quantity
,sum(ifnull(quantity * (s.price - m.wholesale_price),0)) as profit
from inventory as m
left outer join sales as s
on m.product_ID = s.product_ID
group by m.product_id
;

select * from profits;

alter materialized view mv1 suspend;

INSERT INTO inventory (product_ID, wholesale_price, description) VALUES 
    (2, 2.00, 'sprocket');

INSERT INTO sales (product_ID, quantity, price) VALUES 
   (2, 10, 2.99),
   (2,  1, 2.99);

SELECT * FROM profits ORDER BY product_ID;

alter materialized view mv1 resume;

//Clustering a Materialized View
CREATE TABLE pipeline_segments (
    segment_ID BIGINT,
    material VARCHAR, -- e.g. copper, cast iron, PVC.
    installation_year DATE,  -- older pipes are more likely to be corroded.
    rated_pressure FLOAT  -- maximum recommended pressure at installation time.
    );

INSERT INTO pipeline_segments 
    (segment_ID, material, installation_year, rated_pressure)
  VALUES
    (1, 'PVC', '1994-01-01'::DATE, 60),
    (2, 'cast iron', '1950-01-01'::DATE, 120)
    ;

CREATE TABLE pipeline_pressures (
    segment_ID BIGINT,
    pressure_psi FLOAT,  -- pressure in Pounds per Square Inch
    measurement_timestamp TIMESTAMP
    );
    
INSERT INTO pipeline_pressures 
   (segment_ID, pressure_psi, measurement_timestamp) 
  VALUES
    (2, 10, '2018-09-01 00:01:00'),
    (2, 95, '2018-09-01 00:02:00')
    ;

create or replace materialized view vulnerable_pipes
(segment_ID, installation_year, rated_pressure)
as
select segment_ID, installation_year, rated_pressure
from pipeline_segments
where material = 'cast iron' and installation_year < '1980'::date
;

alter materialized view vulnerable_pipes cluster by (installation_year);

select get_ddl('table','vulnerable_pipes');
create or replace materialized view VULNERABLE_PIPES(
	SEGMENT_ID,
	INSTALLATION_YEAR,
	RATED_PRESSURE
) 
cluster by (installation_year) 
as
select segment_ID, installation_year, rated_pressure
from pipeline_segments
where material = 'cast iron' and installation_year < '1980'::date
;


CREATE VIEW high_risk AS
    SELECT seg.segment_ID
         , installation_year
         , measurement_timestamp::DATE AS measurement_date
         , DATEDIFF('YEAR', installation_year::DATE, measurement_timestamp::DATE) AS age
         , rated_pressure - age AS safe_pressure
         , pressure_psi AS actual_pressure
       FROM vulnerable_pipes AS seg INNER JOIN pipeline_pressures AS psi 
           ON psi.segment_ID = seg.segment_ID
       WHERE pressure_psi > safe_pressure
       ;

SELECT * FROM high_risk;


//https://docs.snowflake.com/en/sql-reference/sql/create-pipe
create or replace pipe snowflake_pipe
auto_ingest = true
integration = 'MYINT'
comment = 'Snowflake snowpipe'
as
copy into <target tabe>
from @db.schema.stage_name
file_format = (format_name = 'db.schema.ffname')
;

//Automating Continuous Data Loading Using Cloud Messaging

create or replace table varia (float1 float, v variant, float2 float);
insert into varia(float1, v, float2) values(1.23,NULL,NULL);
select * from varia;

update varia set v = to_variant(float1);
update varia set float2 = v::float;

UPDATE varia SET v = TO_VARIANT(float1);  -- converts FROM a float TO a variant.
UPDATE varia SET float2 = v::FLOAT;       -- converts FROM a variant TO a float.

insert into varia (v)
select to_variant(parse_json('{"key3":"value3", "key4":"value4"}'));

select * from varia;


//Object
select object_construct('name','Jones'::variant, 'age',42::variant);

update varia set v = {'Alberta': 'Edmonton' , 'Manitoba': 'Winnipeg'};
update varia set v = object_construct{'Alberta': 'Edmonton' , 'Manitoba': 'Winnipeg'};

select v['Alberta'] from varia;

create or replace table array_example(array_column array);
insert into array_example (array_column)
select array_construct(12, 'twelve', NULL);
select * from array_example;
select array_column[0] from array_example;

create or replace table test_semi_structured(var variant, arr array, obj object);
describe table test_semi_structured;

create or replace table demonstration1(
 id integer
,array1 array
,variant1 variant
,object1 object
)
;

insert into demonstration1(id, array1, variant1, object1)
select
 1
,array_construct(1,2,3)
,parse_json('{"key1":"value1", "key2":"value2"}')
,PARSE_JSON('{ "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" },'
              ||
               '"outer_key2": { "inner_key2": 2 } } ')
;

INSERT INTO demonstration1 (id, array1, variant1, object1) 
SELECT 
    2,
    ARRAY_CONSTRUCT(1, 2, 3, NULL), 
    PARSE_JSON(' { "key1": "value1", "key2": NULL } '),
    PARSE_JSON(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": NULL }, '
              ||
               '   "outer_key2": { "inner_key2": 2 } '
              ||
               ' } ')
;

select * from demonstration1;


create or replace table json_test(raw variant);

//Objects
insert overwrite into json_test(raw)
select parse_json('{"firstName":"John", "lastName":"Doe"}');

//Array
insert overwrite into json_test(raw)
select parse_json('{"employees":[
    {"firstName":"John", "lastName":"Doe"},
    {"firstName":"Anna", "lastName":"Smith"},
    {"firstName":"Peter", "lastName":"Jones"}
  ]
}');

select * from json_test;
select raw:employees[0] from json_test;

insert overwrite into json_test(raw)
select parse_json('{"root":[{"employees":[
    {"firstName":"John", "lastName":"Doe"},
    {"firstName":"Anna", "lastName":"Smith"},
    {"firstName":"Peter", "lastName":"Jones"}
]}]}');

insert overwrite into json_test(raw)
select 
parse_json('{"root":
   [
    { "kind": "person",
      "fullName": "John Doe",
      "age": 22,
      "gender": "Male",
      "phoneNumber":
        {"areaCode": "206",
         "number": "1234567"},
      "children":
         [
           {
             "name": "Jane",
             "gender": "Female",
             "age": "6"
           },
           {
              "name": "John",
              "gender": "Male",
              "age": "15"
           }
         ],
      "citiesLived":
         [
            {
               "place": "Seattle",
               "yearsLived": ["1995"]
            },
            {
               "place": "Stockholm",
               "yearsLived": ["2005"]
            }
         ]
      },
      {"kind": "person", "fullName": "Mike Jones", "age": 35, "gender": "Male", "phoneNumber": { "areaCode": "622", "number": "1567845"}, "children": [{ "name": "Earl", "gender": "Male", "age": "10"}, {"name": "Sam", "gender": "Male", "age": "6"}, { "name": "Kit", "gender": "Male", "age": "8"}], "citiesLived": [{"place": "Los Angeles", "yearsLived": ["1989", "1993", "1998", "2002"]}, {"place": "Washington DC", "yearsLived": ["1990", "1993", "1998", "2008"]}, {"place": "Portland", "yearsLived": ["1993", "1998", "2003", "2005"]}, {"place": "Austin", "yearsLived": ["1973", "1998", "2001", "2005"]}]},
      {"kind": "person", "fullName": "Anna Karenina", "age": 45, "gender": "Female", "phoneNumber": { "areaCode": "425", "number": "1984783"}, "citiesLived": [{"place": "Stockholm", "yearsLived": ["1992", "1998", "2000", "2010"]}, {"place": "Russia", "yearsLived": ["1998", "2001", "2005"]}, {"place": "Austin", "yearsLived": ["1995", "1999"]}]}
    ]
}');

select * from json_test;
select raw:root from json_test;
select raw:root[0].age from json_test;
select raw:root[0], raw:root[0].age, raw:root[0].children from json_test;

//NULL values
select
 parse_json(NULL) as "SQL NULL"
,parse_json('null') as "JSON NULL"
,parse_json('[null]') as "JSON NULL"
,parse_json('{"a":null}'):a as "JSON NULL"
,parse_json('{"a":null}'):b as "Absent value"
;

select
 parse_json('{"a":null}'):a as "JSON NULL"
,to_char(parse_json('{"a":null}'):a)
;

select column1, to_varchar(parse_json(column1):a)
from values ('{"a":null}')
,('{"b": "hello"}')
,('{"a": "world"}')
;

//Stream
create or replace table orders(id int, order_name varchar);
create or replace table customers(id int, cuatomer_name varchar);
--
create or replace view ordersbycustomer as select * from orders natural join customers;
select * from ordersbycustomer;
--
insert into orders values(1,'order1');
insert into customers values(1, 'customer1');

create or replace stream ordersByCustomerStream on view ordersbycustomer;
select * from ordersByCustomerStream;

insert into orders values(1, 'order2');
insert into customers values(1, 'customer2');

//Stream Examples
--create a table to store the names and fees paid by members of a gym
create or replace table members(
 id number(8) not null
,name varchar(255) default NULL
,fee number(3) null
);

--create a stream to track changes to date in the Members table
create or replace stream member_check on table members;

--create a table to store the dates when gym members joined 
create or replace table signup
(
 id number(8)
,dt date
);

INSERT INTO members (id,name,fee)
VALUES
(1,'Joe',0),
(2,'Jane',0),
(3,'George',0),
(4,'Betty',0),
(5,'Sally',0);

INSERT INTO signup
VALUES
(1,'2018-01-01'),
(2,'2018-02-15'),
(3,'2018-05-01'),
(4,'2018-07-16'),
(5,'2018-08-21');

-- The stream records the inserted rows
select * from member_check;

-- Apply a $90 fee to members who joined the gym after a free trial period ended:
merge into members as t
using(select
 id
,dt
from signup
where datediff(day,'2018-08-15'::date, dt::date) < -30
)s
on s.id = t.id
--
when matched then update set t.fee = 90
;

-- The stream records the updated FEE column as a set of inserts
-- rather than deletes and inserts because the stream contents
-- have not been consumed yet
select * from member_check;

-- Create a table to store member details in production
CREATE OR REPLACE TABLE members_prod (
  id number(8) NOT NULL,
  name varchar(255) default NULL,
  fee number(3) NULL
);

-- Insert the first batch of stream data into the production table
insert into members_prod(id, name, fee)
select id, name, fee 
from member_check
where METADATA$ACTION = 'INSERT'
;

-- The stream position is advanced
select * from member_check;

-- Access and lock the stream
BEGIN;

--Increase the fee paid by paying members
update members set fee = fee + 15
where fee > 0
;

-- These changes are not visible because the change interval of the stream object starts at the current offset and ends at the current
-- transactional time point, which is the beginning time of the transaction
select * from member_check;

--commit changes
commit;

-- The changes surface now because the stream object uses the current transactional time as the end point of the change interval that now
-- includes the changes in the source table
SELECT * FROM member_check;

//Differences Between Standard and Append-only Streams
//The following example shows the differences in behavior between standard (delta) and append-only streams:
--create a source table.
create or replace table t(id int, name string);

--Create a standard stream on the source table
create or replace stream delta_s on table t;

--Create a append-only stream on the source table
create or replace stream append_only_s on table t append_only=true;

-- Insert 3 rows into the source table.
insert into t values (0, 'charlie brown');
insert into t values (1, 'lucy');
insert into t values (2, 'linus');

--delete 1 of the 3 rows
delete from t where id = '0';

--The standard stream removes the deleted rows
select * from delta_s order by id;

--The append-only stream does not remove the deleted row.
select * from append_only_s order by id;

-- Create a table to store the change data capture records in each of the streams.
create or replace  table t2(id int, name string, stream_type string default NULL);

-- Insert the records from the streams into the new table, advancing the offset of each stream.
insert into t2(id, name, stream_type) select id, name, 'delta stream' from delta_s;
insert into t2(id, name, stream_type) select id, name, 'append_only stream' from append_only_s;

-- Update a row in the source table.
select * from t;
update t set name = 'sally' where name = 'linus';

--The standard stream records the update operation.
select * from delta_s order by id;

-- The append-only stream does not record the update operation.
select * from append_only_s order by id;




//DML Operations in Explicit Transactions
Create or replace table data_staging (raw variant);

--Create a stream on the staging table
create or replace stream data_check on table data_staging;

--Create 2 production tables to store transformed JSON data in relational columns
create or replace table data_prod1
(id number(8)
,ts timestamp_tz
)
;

create or replace table data_prod2
(id number(8)
,color varchar
,num number
)
;

-- Load JSON data into staging table
-- using COPY statement, Snowpipe,
-- or inserts

insert into data_staging(raw)
select parse_json('{                                   
  "id": 7077,                       
  "x1": "2018-08-14T20:57:01-07:00",
  "x2": [                           
    {                               
      "y1": "green",                
      "y2": "35"                    
    }                               
  ]                                 
}') raw;

insert into data_staging(raw)
select parse_json('                           
{                                   
  "id": 7078,                       
  "x1": "2018-08-14T21:07:26-07:00",
  "x2": [                           
    {                               
      "y1": "cyan",                 
      "y2": "107"                   
    }                               
  ]                                 
}') raw
;

select * from data_staging;

--  Stream table shows inserted data
SELECT * FROM data_check;

-- Access and lock the stream
BEGIN;

--transform and copy JSON elements into relational columns
--in the production tables
insert into data_prod1(id, ts)
select raw:id
,to_timestamp_tz(raw:x1)
from data_check
where metadata$action = 'INSERT'
;

insert into data_prod2(id,color,num)
select
 raw:id::number
,f.value:y1::string
,f.value:y2::number
from data_check as t
,lateral flatten(input=>raw:x2) as f
where metadata$action = 'INSERT'
;

-- Commit changes in the stream objects participating in the transaction
commit;

select * from data_prod1;
select * from data_prod2;
SELECT * FROM data_check;


//Streams on Views
//Stream on a View with Multi-table Joins
--Create multiple tables with matching column values
create table birds(
 id number
,common varchar(100)
,class varchar(100)
)
;

create table sightings(
 d date
,loc varchar(100)
,b_id number
,c number
);

--Create a view that queries tha tables with a join
create view birds_sightings
as
select
 b.id as id
,b.common as common_name
,b.class as classification
,s.d as date
,s.loc as location
,s.c as count
from birds as b
inner join sightings s 
on b.id = s.b_id
;

--Create a stream on the view
create stream birds_sightings_s on view birds_sightings;

-- Insert values into the tables.
INSERT INTO birds
VALUES
    (1,'Scarlet Tanager','P. olivacea'),
    (14,'Mallard','A. platyrhynchos'),
    (48,'Spotted Sandpiper','A. macularius'),
    (92,'Great Blue Heron','A. herodias');

INSERT INTO sightings
VALUES
    (current_date(),'Gibson Island',1,4),
    (current_date(),'Lake Los Pajaro',14,12),
    (current_date(),'Lake Los Pajaro',92,12),
    (current_date(),'Gibson Island',14,21),
    (current_date(),'Gibson Island',92,5);

-- Query the stream.
-- The stream displays a record for each row added to the view.
SELECT * FROM birds_sightings_s;

-- Consume the stream records in a DML statement (INSERT, MERGE, etc.).
create or replace table bird_sightings_t
as select * from birds_sightings_s;

-- Query the stream.
-- The stream is empty.
SELECT * FROM birds_sightings_s;

--Delete a row from the birds table 
delete from birds where id = 14;

--query the stream
--the stream displays two records for the single DELETE operation
SELECT * FROM birds_sightings_s;



//Stream on a View That Calls a Non-deterministic SQL Function
--Create a table
create table ndf
(
c1 number
)
;

-- Create a view that queries the table and
-- also returns the CURRENT_USER and CURRENT_TIMESTAMP values
-- for the query transaction.

create view ndf_v as 
select 
current_user() as u
,current_timestamp() as ts
,c1 as num
from ndf
;

--create a stream on the view
create stream ndf_s on view ndf_v;

--user xx inserts rows into table ndf
insert into ndf
values (1)
,(2)
,(3)
;

-- User marie inserts rows into table ndf.
INSERT INTO ndf
VALUES
    (4),
    (5),
    (6);

-- User PETER queries the stream.
-- The stream returns the username for the user.
-- The stream also returns the current timestamp for the query transaction in each row,
-- NOT the timestamp when each row was inserted.
SELECT * FROM ndf_s;

-- User MARIE queries the stream.
-- The stream returns the username for the user
-- and the current timestamp for the query transaction in each row.
SELECT * FROM ndf_s;


//Creating an Insert-only Stream on an External Table
show stages in account;

list @UTIL_DB.PUBLIC.AWS_S3_BUCKET;
list @DEMO_DB.PUBLIC.SFQUICKSTARTS_EXT_STG;
list @HOL_DB.HOL_SCHEMA.FROSTBYTE_RAW_STAGE;
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE;
list @SF_INT_STG;

select * from data_staging;

copy into @SF_INT_STG/2024-01-12_data.json
from data_staging
file_format = (type = 'JSON')
OVERWRITE = TRUE
SINGLE = TRUE
;

-- Create an external table that points to the MY_EXT_STAGE stage.
-- The external table is partitioned by the date (in YYYY/MM/DD format) in the file path.
CREATE EXTERNAL TABLE my_ext_table (
  date_part date as to_date(substr(metadata$filename, 1, 10), 'YYYY/MM/DD'),
  ts timestamp AS (value:time::timestamp),
  user_id varchar AS (value:userId::varchar),
  color varchar AS (value:color::varchar)
) PARTITION BY (date_part)
  LOCATION=@my_ext_stage
  AUTO_REFRESH = false
  FILE_FORMAT=(TYPE=JSON);

-- Create a stream on the external table
CREATE STREAM my_ext_table_stream ON EXTERNAL TABLE my_ext_table INSERT_ONLY = TRUE;

-- Execute SHOW streams
-- The MODE column indicates that the new stream is an INSERT_ONLY stream
SHOW STREAMS;

-- Add a file named '2020/08/05/1408/log-08051409.json' to the stage using the appropriate tool for the cloud storage service.

-- Manually refresh the external table metadata.
ALTER EXTERNAL TABLE my_ext_table REFRESH;

-- Query the external table stream.
-- The stream indicates that the rows in the added JSON file were recorded in the external table metadata.
SELECT * FROM my_ext_table_stream;


//Creating a Standard Stream on a Directory Table
show stages in account;

list @UTIL_DB.PUBLIC.AWS_S3_BUCKET;
list @DEMO_DB.PUBLIC.SFQUICKSTARTS_EXT_STG;
list @HOL_DB.HOL_SCHEMA.FROSTBYTE_RAW_STAGE;
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE;
list @SF_INT_STG;
ALTER STAGE IF EXISTS SF_INT_STG SET DIRECTORY = ( ENABLE = TRUE);

create or replace stream irtable_mystage_s ON STAGE SF_INT_STG;
show streams like 'irtable_mystage_s';

select * from irtable_mystage_s;

//Manually refresh the directory table metadata to populate the stream:
alter stage SF_INT_STG refresh;

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;


//Transforming Loaded JSON Data on a Schedule
-- Create a landing table to store raw JSON data.
-- Snowpipe could load data into this table.
create or replace table raw (var variant);

-- Create a stream to capture inserts to the landing table.
-- A task will consume a set of columns from this stream.
create or replace stream rawstream1 on table raw;

-- Create a second stream to capture inserts to the landing table.
-- A second task will consume another set of columns from this stream.
create or replace stream rawstream2 on table raw;

-- Create a table that stores the names of office visitors identified in the raw data.
create or replace table names (id int, first_name string, last_name string);

-- Create a table that stores the visitation dates of office visitors identified in the raw data.
create or replace table visits (id int, dt date);

-- Create a task that inserts new name records from the rawstream1 stream into the names table
-- every minute when the stream contains records.
-- Replace the 'mywh' warehouse with a warehouse that your role has USAGE privilege on.
create or replace task raw_to_names
warehouse = compute_wh
schedule = '1 minute'
when 
system$stream_has_data('rawstream1')
as
merge into names n
  using (select var:id id, var:fname fname, var:lname lname from rawstream1) r1 on n.id = to_number(r1.id)
  when matched then update set n.first_name = r1.fname, n.last_name = r1.lname
  when not matched then insert (id, first_name, last_name) values (r1.id, r1.fname, r1.lname)
;

select system$stream_has_data('rawstream1');

-- Create another task that merges visitation records from the rawstream1 stream into the visits table
-- every minute when the stream contains records.
-- Records with new IDs are inserted into the visits table;
-- Records with IDs that exist in the visits table update the DT column in the table.
-- Replace the 'mywh' warehouse with a warehouse that your role has USAGE privilege on.
create or replace task raw_to_visits
warehouse = compute_wh
schedule = '1 minute'
when 
system$stream_has_data('rawstream2')
as
merge into visits v
  using (select var:id id, var:visit_dt visit_dt from rawstream2) r2 on v.id = to_number(r2.id)
  when matched then update set v.dt = r2.visit_dt
  when not matched then insert (id, dt) values (r2.id, r2.visit_dt)
;

show tasks like 'raw_to%';

--Resume both tasks
alter task raw_to_names resume;
alter task raw_to_visits resume;
--
alter task raw_to_names suspend;
alter task raw_to_visits suspend;

-- Insert a set of records into the landing table.
insert into raw
  select parse_json(column1)
  from values
  ('{"id": "123","fname": "Jane","lname": "Smith","visit_dt": "2019-09-17"}'),
  ('{"id": "456","fname": "Peter","lname": "Williams","visit_dt": "2019-09-17"}');

-- Query the change data capture record in the table streams
select * from rawstream1;
select * from rawstream2;

-- Wait for the tasks to run.
-- A tiny buffer is added to the wait time
-- because absolute precision in task scheduling is not guaranteed.
call system$wait(70);

-- Query the table streams again.
-- Records should be consumed and no longer visible in streams.
select * from rawstream1;
select * from rawstream2;

-- Insert another set of records into the landing table.
-- The records include both new and existing IDs in the target tables.
insert into raw
  select parse_json(column1)
  from values
  ('{"id": "456","fname": "Peter","lname": "Williams","visit_dt": "2019-09-25"}'),
  ('{"id": "789","fname": "Ana","lname": "Glass","visit_dt": "2019-09-25"}');

-- Wait for the tasks to run.
call system$wait(70);

-- Records should be consumed and no longer visible in streams.
select * from rawstream1;
select * from rawstream2;

-- Verify the records were inserted into the target tables.
select * from names;
select * from visits;

select * from table(information_schema.task_history(
scheduled_time_range_start=>dateadd('day',-2,current_timestamp()),
task_name=>'raw_to_names'));

select *
from table(information_schema.task_history(
scheduled_time_range_start=>dateadd('day',-2,current_timestamp()),
task_name=>'raw_to_visits'));

show tasks like 'raw_to_%' in account;

//Unloading Data on a Schedule
-- Use the landing table from the previous example.
-- Alternatively, create a landing table.
-- Snowpipe could load data into this table.
create or replace table raw (id int, type string);

-- Create a stream on the table.  We will use this stream to feed the unload command.
create or replace stream rawstream on table raw;

-- Create a task that executes the COPY statement every minute.
-- The COPY statement reads from the stream and loads into the table stage for the landing table.
-- Replace the 'mywh' warehouse with a warehouse that your role has USAGE privilege on.
create or replace task unloadtask
warehouse = compute_wh
schedule = '1 minute'
when
  system$stream_has_data('RAWSTREAM')
as
copy into @SF_INT_STG/rawstream from rawstream overwrite=true;
;

-- Resume the task.
alter task unloadtask resume;
alter task unloadtask suspend;

-- Insert raw data into the landing table.
insert into raw values (3,'processed');

-- Query the change data capture record in the table stream
select * from rawstream;

-- Wait for the tasks to run.
-- A tiny buffer is added to the wait time
-- because absolute precision in task scheduling is not guaranteed.
call system$wait(70);

-- Records should be consumed and no longer visible in the stream.
select * from rawstream;

-- Verify the COPY statement unloaded a data file into the table stage.
ls @SF_INT_STG/rawstream;

select * from table(information_schema.task_history(
scheduled_time_range_start=>dateadd('day',-2,current_timestamp()),
task_name=>'unloadtask'));

//Refreshing External Table Metadata on a Schedule
-- Create a task that executes an ALTER EXTERNAL TABLE ... REFRESH statement every 5 minutes.
-- Replace the 'mywh' warehouse with a warehouse that your role has USAGE privilege on.
CREATE TASK exttable_refresh_task
WAREHOUSE=mywh
SCHEDULE='5 minutes'
  AS
ALTER EXTERNAL TABLE mydb.myschema.exttable REFRESH;

//Suppose that the root task for a DAG is task1 and that task2, task3, and task4 are child tasks of task1. This example adds child task task5 to the DAG and specifies task2, task3, and task4 as predecessor tasks:
-- Create task5 and specify task2, task3, task4 as predecessors tasks.
-- The new task is a serverless task that inserts the current timestamp into a table column.
CREATE TASK task5
user_task_managed_initial_warehouse_size = 'xsmall'
  AFTER task2, task3, task4
AS
INSERT INTO t1(ts) VALUES(CURRENT_TIMESTAMP);

























































































































        