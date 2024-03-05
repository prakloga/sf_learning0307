//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
//Prerequisite
create or replace table demo_db.public.employee_table (
    employee_ID INTEGER,
    last_name VARCHAR,
    first_name VARCHAR,
    department_ID INTEGER
    );

INSERT INTO demo_db.public.employee_table (employee_ID, last_name, first_name, department_ID) VALUES
    (101, 'Montgomery', 'Pat', 1),
    (102, 'Levine', 'Terry', 2),
    (103, 'Comstock', 'Dana', 2);

create or replace table demo_db.public.department_table (
    department_ID INTEGER,
    department_name VARCHAR
    );

INSERT INTO demo_db.public.department_table (department_ID, department_name) VALUES
    (1, 'Engineering'),
    (2, 'Customer Support'),
    (3, 'Finance');

select * from demo_db.public.employee_table;
select * from demo_db.public.department_table;

/*-------------------------------------------------------------------------------------*/
//1.Selecting All Columns With Names That Match a Pattern
/*
employee_ID
,last_name
,first_name
,department_ID
*/
--New method
select * ilike '%name' from demo_db.public.employee_table;

--classic method
select first_name, last_name from demo_db.public.employee_table;
select * from demo_db.public.employee_table;


//2.A.Selecting All Columns Except One Column | B.Selecting All Columns Except Two or More Columns
/*
ID
,LASTNAME
,FIRSTNAME
,COMPANY
,EMAIL
,WORKPHONE
,CELLPHONE
,STREETADDRESS
,CITY
,POSTALCODE
*/
--NEW method
select * EXCLUDE CELLPHONE from demo_db.public.contacts;
select * EXCLUDE (EMAIL, CELLPHONE) from demo_db.public.contacts;

--classic method
select 
ID
,LASTNAME
,FIRSTNAME
,COMPANY
,EMAIL
,WORKPHONE
--,CELLPHONE
,STREETADDRESS
,CITY
,POSTALCODE
from demo_db.public.contacts
;



//3.A.Selecting All Columns and Renaming One Column | b.Selecting All Columns and Renaming Multiple Columns
--NEW method
select * RENAME department_id as department from demo_db.public.employee_table;
select * RENAME (department_id as department, employee_id as employee) from demo_db.public.employee_table;

--classic method
select
 employee_id
--employee_id as employee
,last_name
,first_name
,department_id as department
from demo_db.public.employee_table
;



//4.Selecting All Columns From Multiple Tables, Excluding a Column, and Renaming a Column
--NEW Method
SELECT
 employee_table.* EXCLUDE department_id
,department_table.* RENAME department_name AS department
FROM demo_db.public.employee_table 
INNER JOIN demo_db.public.department_table
  ON employee_table.department_id = department_table.department_id
ORDER BY department, last_name, first_name
;


--classic Method
SELECT
 employee_table.EMPLOYEE_ID
,employee_table.LAST_NAME
,employee_table.FIRST_NAME
,department_table.DEPARTMENT_ID
,department_table.department_name AS department
FROM demo_db.public.employee_table 
INNER JOIN demo_db.public.department_table
ON employee_table.department_id = department_table.department_id
ORDER BY department, last_name, first_name
;



/*------------------------------------------------------------------------------------*/
//GROUP BY ALL
--https://docs.snowflake.com/en/sql-reference/constructs/group-by
CREATE OR REPLACE TABLE demo_db.public.sales(
  product_ID INTEGER,
  retail_price REAL,
  quantity INTEGER,
  city VARCHAR,
  state VARCHAR);

INSERT INTO demo_db.public.sales (product_id, retail_price, quantity, city, state) VALUES
  (1, 2.00,  1, 'SF', 'CA'),
  (1, 2.00,  2, 'SJ', 'CA'),
  (2, 5.00,  4, 'SF', 'CA'),
  (2, 5.00,  8, 'SJ', 'CA'),
  (2, 5.00, 16, 'Miami', 'FL'),
  (2, 5.00, 32, 'Orlando', 'FL'),
  (2, 5.00, 64, 'SJ', 'PR');

CREATE OR REPLACE TABLE demo_db.public.products (
  product_ID INTEGER,
  wholesale_price REAL);
  
INSERT INTO demo_db.public.products (product_ID, wholesale_price) VALUES (1, 1.00), (2, 2.00);

--Validation
select * from demo_db.public.sales;
select * from demo_db.public.products;

/*------------------------------------------------------------------------------------*/
//Group By Multiple Columns
--New Method
select
 state
,city
,sum(retail_price * quantity) as gross_revenue
from demo_db.public.sales
group by all
;


--classic Method
select
 state
,city
,sum(retail_price * quantity) as gross_revenue
from demo_db.public.sales
group by state, city
--group by 1,2
;

