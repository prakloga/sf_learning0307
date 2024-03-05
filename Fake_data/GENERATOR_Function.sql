//set worksheet context
use role sysadmin;
use warehouse compute_wh;
use schema demo_db.fake_data;
/*-----------------------------------------------------------------------------*/
//Medium post:
--https://towardsdatascience.com/how-to-create-synthetic-rows-with-snowflakes-generator-function-8f947d40d9b3

//GENERATOR
--https://docs.snowflake.com/en/sql-reference/functions/generator#syntax
--https://docs.snowflake.com/en/sql-reference/functions-data-generation#data-generation-functions

//Basics
select 'prakash' from table(generator(rowcount => 10));
select 'prakash' from table(generator(timelimit => 1));

--#1 Generate Sequential IDs
/*
Below you find 4 ways to create sequential IDs with GENERATOR
*/
select
 row_number() over(order by 1 desc) as gen_id_v1
,1000+row_number() over(order by 1 desc) as gen_id_v2
,'ABC'||'-'||to_char(1000+row_number() over(order by 1 desc)) as gen_id_v3
,uuid_string() as gen_id_v4
from table(generator(rowcount=>25))
;

--#2 Generate String Attributes
/*Generate random string attributes like PLATFORM, PRODUCT, CUSTOMER_NAME and many more.
  In Snowflake this can be achieved by using GENERATOR in combination with the UNIFORM and RANDOM functions.
*/
select
 array_construct('MOBILE','LAPTOP','TABLET')[uniform(0,2,random())]::string as gen_platform
,case uniform(1,3,random())
    when 1 then 'PRODUCT_A'
    when 2 then 'PRODUCT_B'
    when 3 then 'PRODUCT_C'
 end as gen_product
,case uniform(1,3,random())
    when 1 then 'Jennifer Garner'
    when 2 then 'Ben Affleck'
    when 3 then 'Jennifer Lopez'
 end as gen_customer_name
from table(generator(rowcount=>10))
;

--#3 Generate Dates And Timestamps
/*
For instance, the query below returns the START and END dates for each month in 2022:
*/
--https://docs.snowflake.com/en/sql-reference/functions/dateadd
--https://docs.snowflake.com/en/sql-reference/functions/last_day
select
 dateadd('month',row_number() over(order by 1),'2022-12-01')::date as gen_month_start //pass one month prior date to get correct value
,last_day(dateadd('month',row_number() over(order by 1),'2022-12-01')::date) as gen_month_end
from table(generator(rowcount=>12))
order by gen_month_start asc
;

/*
To randomly produce timestamps, you could again take advantage of data generation functions and pass them as arguments of DATEADD, as shown below:
*/
select
 dateadd('minutes', uniform(1,60,random()),current_timestamp()) as gen_rand_timestamp_v1
,dateadd('days',uniform(1,15,random()),gen_rand_timestamp_v1) as gen_rand_timestamp_v2
from table(generator(rowcount=>15))
order by 2
;


--#4Final Example: Generate A Date Template Table
/*
In case, you were wondering how to create them with the help of the GENERATOR function,
below you find a script that builds such auxiliary date template table for year 2022:
*/
select
dateadd('day',row_number() over(order by 1),'2022-12-31')::date as gen_date
,extract(year from gen_date) as gen_year
,extract(month from gen_date) as gen_month
,extract(day from gen_date) as gen_day
,extract(dayofweek from gen_date) as gen_dow
,extract(dayofyear from gen_date) as gen_doy
,extract(quarter from gen_date) as gen_quarter
,case gen_dow
 when 0 then 'Sunday'
 when 1 then 'Monday'
 when 2 then 'Tuesday'
 when 3 then 'Wednesday'
 when 4 then 'Thursday'
 when 5 then 'Friday'
 when 6 then 'Saturday' end as gen_day_name
from table(generator(rowcount=>365))
order by gen_date
;

/*------------------------------------------------------------------------------------------------*/
//Medium
--https://medium.com/snowflake/generating-realistic-looking-fake-data-in-snowflake-80796c77adb3
--https://github.com/jamesweakley/flaker
--https://medium.com/snowflake/flaker-2-0-fake-snowflake-data-the-easy-way-dc5e65225a13

create or replace function FAKE(locale varchar,provider varchar,parameters variant)
returns variant
language python
volatile
runtime_version = '3.10'
packages = ('faker','simplejson')
handler = 'fake'
as
$$
import simplejson as json
from faker import Faker

def fake(locale,provider,parameters):
  if type(parameters).__name__=='sqlNullWrapper':
    parameters = {}
  fake = Faker(locale=locale)
  return json.loads(json.dumps(fake.format(formatter=provider,**parameters), default=str))
$$
;

--https://faker.readthedocs.io/en/master/providers.html
--Generate 50 fake names, in the US English locale:
select FAKE('en_US','name',{})::string as FAKE_NAME
from table(generator(rowcount => 50))
;

--Generate 50 fake random dates
select FAKE('en_US','date_time',{})::date as FAKE_date
from table(generator(rowcount => 50))
;

--Generate 50 fake US address
select FAKE('en_US','address',{})::string as FAKE_address
from table(generator(rowcount => 50))
;

--100 dates between 180 days ago and today:
select FAKE('en_US','date_between',{'start_date':'-180d','end_date':'today'})::date as FAKE_DATE
 from table(generator(rowcount => 50));

/*------------------------------------------------------------------------------------*/
//https://www.snowflake.com/blog/synthetic-data-generation-at-scale-part-1/
--The following statement creates 10 rows, each row having one column with the static value of 1.
select 1 from table(generator(rowcount=>10));

--Each time random() is called, it returns a random 64-bit integer value.
select random() as col1 from table(generator(rowcount=>10));

--So far so good. But most likely, you want to generate more than one random value in a single SQL statement. 
select
 random() as col1
,random() as col2
from table(generator(rowcount=>10))
;

--Using unique seed values for each call to the random() function ensures that each call to random()returns a different value.
select
 random(1) as col1
,random(2) as col2
from table(generator(rowcount=>10))
;

--The uniform() function takes three parameters: min to specify a minimum value, max to specify a maximum value, and gen, which is a generator expression. 
/*
A static generator function produces the same value over and over within the range of min and max. Therefore, we will use the random() function, which generates random values, and as a result, the uniform() function will return random values between min and max.
*/
select
uniform(1,10,random(1)) as col1
from table(generator(rowcount=>100))
;

--In Snowflake, random strings can be generated through the randstr function, which accepts two parameters. The first parameter is the length of the string, and the second is the generator expression.

/*
The same pattern also applies for the randstr() function. A static generator expression produces a static string. For that reason, we’ll use the random function to produce random strings through randstr().

The following statement puts all of these building blocks together.

The first parameter specifies the length of the string to be generated. In this case, it’s a random value with a range from 3 to 10; in other words, we are creating variable length strings of 3 to 10 characters. The second parameter, the generator expression, is a random number with a range of 1 to 100. This means we are generating 100 different strings with a variable length of 3 to 10 characters. 
*/
select
randstr(uniform(3,10,random(1)), uniform(1,100,random(1))) as col1
from table(generator(rowcount=>10))
;


select 
   randstr(uniform(10,30,random(1)),uniform(1,100000,random(1)))::varchar(30) as name
  ,randstr(uniform(10,30,random(2)),uniform(1,10000,random(2)))::varchar(30) as city
  ,randstr(10,uniform(1,100000,random(3)))::varchar(10) as license_plate
  ,randstr(uniform(10,30,random(4)),uniform(1,200000,random(4)))::varchar(30) as email
from table(generator(rowcount=>10));


--https://www.snowflake.com/blog/synthetic-data-generation-at-scale-part-2/
SELECT
(seq8()+1)::bigint as id,
dateadd(day, uniform(1, 365, random(10002)), date_trunc(day, current_date))::date as order_date
from table(generator(rowcount => 500))
order by 2
;






 

