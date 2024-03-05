use schema demo_db.public;
/*
Example: Dropping and Restoring a Table Multiple Times
In the following example, the mytestdb.public schema contains two tables: loaddata1 and proddata1. The loaddata1 table is dropped and recreated twice, creating three versions of the table:

Current version

Second (i.e. most recent) dropped version

First dropped version

The example then illustrates how to restore the two dropped versions of the table:

First, the current table with the same name is renamed to loaddata3. This enables restoring the most recent version of the dropped table, based on the timestamp.

Then, the most recent dropped version of the table is restored.

The restored table is renamed to loaddata2 to enable restoring the first version of the dropped table.

Lastly, the first version of the dropped table is restored.
*/
create or replace table loaddata1 (id number);
insert into loaddata1 values(100),(101),(102),(103);
select * from loaddata1;

create or replace table proddata1 (id number);
insert into proddata1 values(1000),(1001),(1002),(1003);
select * from proddata1;

show tables history in demo_db.public;

drop table loaddata1;

show tables history like 'loaddata1' in demo_db.public;

create or replace table loaddata1 (id number);
INSERT INTO loaddata1 VALUES (1111), (2222), (3333), (4444);

show tables history like 'loaddata1' in demo_db.public;

DROP TABLE loaddata1;
CREATE TABLE loaddata1 (c1 varchar);

show tables history like 'loaddata1' in demo_db.public;
select * from loaddata1;

alter table loaddata1 rename to loaddata3;

undrop table loaddata1;
select * from loaddata1;
show tables history like 'loaddata%' in demo_db.public;

alter table loaddata1 rename to loaddata2;
undrop table loaddata1;
select * from loaddata1;
show tables history like 'loaddata%' in demo_db.public;


//Query history understanding:
select count(*) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION; --metadata based result and did not trigger VWH

select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION order by r_name;

//1st run
select r_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

//2nd run
select r_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;


//1: Remote storage
select s_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;

//2: Metadata cache
select count(*) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;
select min(S_ACCTBAL) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;
select max(S_ACCTBAL) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;
select avg(S_ACCTBAL) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;
select count(S_ACCTBAL) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;

//3: result cache
select s_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;

//4: warehouse cache
select s_name as supplier_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;











