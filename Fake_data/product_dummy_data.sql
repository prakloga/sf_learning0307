//set worksheet context
use role sysadmin;
use warehouse compute_wh;
use schema demo_db.fake_data;
/*-----------------------------------------------------------------------------*/
create or replace table sales_table
comment = 'Sales table'
as
select
 product_id
,sale_date
,daily_sales
from(select
 array_construct(1000,2000,3000,4000,5000)[uniform(0,4,random())]::integer as product_id
,dateadd('days',row_number() over(order by 1),'2022-12-31')::date as gen_date
,dateadd('days',uniform(1,15,random()),gen_date)::date as sale_date
,uniform(10000,50000,random())::decimal(10,2) as daily_sales
from table(generator(rowcount=>500))
)dt
;
--
create or replace table sales_table
comment = 'Sales table'
as
select
 array_construct(1000,2000,3000,4000,5000)[uniform(0,4,random())]::integer as product_id
,dateadd(day, uniform(1, 365, random(10002)), date_trunc(day, '2023-01-01'::date))::date as sale_date
,uniform(10000,50000,random())::decimal(10,2) as daily_sales
from table(generator(rowcount=>1000))
;



//Validation
select * from demo_db.fake_data.sales_table order by product_id,sale_date;
