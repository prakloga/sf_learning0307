//In this quickstart we will build a change data capture (CDC) pipeline, cumulative sum using Python UDTF data validation using Dynamic tables.
--https://docs.snowflake.com/user-guide/dynamic-tables-about
--https://github.com/Snowflake-Labs/sfquickstarts/blob/3265864b0dc9905c4306d975144693d1b388ee00/site/sfguides/src/getting_started_with_dynamic_tables/getting_started_with_dynamic_tables.md?plain=1#L232

use role sysadmin;
use warehouse compute_wh;
/*-------------------------------------------------------------------*/
CREATE DATABASE IF NOT EXISTS DEMO_DB;
CREATE SCHEMA IF NOT EXISTS DEMO_DB.DT_DEMO;
USE SCHEMA DEMO_DB.DT_DEMO;

/*-------------------------------------------------------------------*/
//Check if there is data in all 3 raw tables 
-- customer information table, each customer has spending limits
select * from cust_info limit 10;

-- product stock table, each product has stock level from fulfilment day
select * from prod_stock_inv limit 10;

-- sales data for products purchsaed online by various customers
select * from salesdata limit 10;

/*-------------------------------------------------------------------*/
//3. Build data pipeline using Dynamic Tables
use role sysadmin;
use warehouse compute_wh;
use schema demo_db.dt_demo;

/*
1.For this we will extract the sales information from the salesdata table and join it with customer information to build the customer_sales_data_history, 
note that we are extracting raw json data(schema on read) and transforming it into meaningful columns and data type
*/
create or replace dynamic table customer_sales_data_history
lag="DOWNSTREAM"
warehouse="COMPUTE_WH"
as
select
 s.custid as customer_id
,c.cname as customer_name
,s.purchase:prodid::number(5) as product_id
,s.purchase:purchase_amount::number(10) as saleprice
,s.purchase:quantity::number(5) as quantity
,s.purchase:purchase_date::date as salesdate
from cust_info as c
inner join salesdata as s
on c.custid = s.custid
;

//Quick sanity checks
select * from customer_sales_data_history limit 10;
select count(*) from customer_sales_data_history;



/*
2. Now, let's combine these results with the product table and create a SCD TYPE 2 transformation using window the function "LEAD"
, it gives us the subsequent rows in the same result set to build a TYPE 2 transformation.
*/
create or replace dynamic table salesreport
lag = '1 MINUTE'
warehouse = "COMPUTE_WH"
as
select
 t1.customer_id
,t1.customer_name
,t1.product_id
,t2.pname as product_name
,t1.saleprice
,t1.quantity
,(t1.saleprice/t1.quantity) as unitsaleprice
,t1.salesdate as creationtime
,concat_ws('-',t1.customer_id, t1.product_id, t1.salesdate) as customer_sk
,lead(creationtime) over(partition by t1.customer_id order by creationtime asc) as end_time
from customer_sales_data_history as t1
inner join prod_stock_inv as t2
on t1.product_id = t2.pid
;

//Quick sanity checks
select * from salesreport limit 100;
select count(*) from salesreport; -- 10000



/*
3. Test: Let's test this DAG by adding some raw data in the base tables.
*/
//Before
select count(*) from salesdata; --10000
select count(*) from customer_sales_data_history; --10000
select count(*) from salesreport; --10000

//After
-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));

--Check raw base table
select count(*) from salesdata; --20000

--Check dynamic tables after a minute
select count(*) from customer_sales_data_history; --20000
select count(*) from salesreport; --20000

//MANUAL REFRESH
SHOW DYNAMIC TABLES IN SCHEMA "DEMO_DB"."DT_DEMO";
ALTER DYNAMIC TABLE customer_sales_data_history REFRESH;
ALTER DYNAMIC TABLE customer_sales_data_history SUSPEND;
ALTER DYNAMIC TABLE customer_sales_data_history RESUME;
--
ALTER DYNAMIC TABLE salesreport REFRESH;
ALTER DYNAMIC TABLE salesreport SUSPEND;
ALTER DYNAMIC TABLE salesreport RESUME;


/*
4.In below example, we'll demonstrate how to build a cumulative total of customer account balances each month and leverage this information to identify any instances of customers exceeding their set limits in the CUST_INFO table.
*/
--This function computes the cumulative total and can be seamlessly incorporated into any SQL code or applied to any table as a table function
CREATE OR REPLACE FUNCTION sum_table (INPUT_NUMBER number)
  returns TABLE (running_total number)
  language python
  runtime_version = '3.10'
  handler = 'gen_sum_table'
as
$$

# Define handler class
class gen_sum_table :

  ## Define __init__ method to initilize the variable
  def __init__(self) :    
    self._running_sum = 0
  
  ## Define process method
  def process(self, input_number: float) :
    # Increment running sum with data from the input row
    new_total = self._running_sum + input_number
    self._running_sum = new_total

    yield(new_total,)
  
$$
;

/*
5. It's flexibile and allows us to feed any data partition, making it highly adaptable to any "cumulative total" use case
  .Let's partition this total by Customer and Month using dynamic table. This way it becomes highly modular and SQL independent.
*/
//Option-1
create or replace dynamic table cumulative_purchase
lag = '1 minute'
warehouse="COMPUTE_WH"
as
select
 monthNum
,yearNum
,customer_id
,saleprice
,running_total
from(select 
    month(creationtime) monthNum,
    year(creationtime) yearNum,
    customer_id, 
    saleprice,
    date_trunc('month',creationtime) as yearmonth
    from salesreport
   )dt
,table(sum_table(saleprice) over (partition by customer_id, yearmonth order by customer_id, yearmonth))
;

//Option-2
create or replace dynamic table cumulative_purchase
lag = '1 minute'
warehouse="COMPUTE_WH"
as
select 
month(creationtime) monthNum,
year(creationtime) yearNum,
customer_id, 
saleprice,
sum(saleprice) over(partition by customer_id, date_trunc('month',creationtime) order by customer_id, date_trunc('month',creationtime) rows between unbounded preceding and current row) as running_total 
from salesreport
--where customer_id = 1652
;

//Validation
ALTER DYNAMIC TABLE cumulative_purchase REFRESH;
ALTER DYNAMIC TABLE cumulative_purchase SUSPEND;
ALTER DYNAMIC TABLE cumulative_purchase RESUME;

//Quick sanity check
select * from cumulative_purchase limit 10;
select * from cumulative_purchase where customer_id = 1652 order by running_total; 

/*
6. In our data set, we want to know if a product is running low on inventory, let's say less than 10%
*/
create or replace dynamic table prod_inv_alert
lag='1 MINUTE'
warehouse="COMPUTE_WH"
as
select
 s.product_id
,s.product_name
,s.creationtime as latest_sale_date
,p.stock as begining_stock
,sum(s.quantity) over(partition by s.product_id order by s.creationtime) as totalunitsold
,(p.stock - totalunitsold) as unitsleft
,round(((p.stock - totalunitsold)/p.stock * 100),2) as percent_unitleft
,current_timestamp() as rowcreationtime
from salesreport as s
inner join prod_stock_inv as p
on s.product_id =  p.pid
--where s.product_id = 155
qualify row_number() over(partition by s.product_id order by s.creationtime desc) = 1 //very recent sale event
;

//Quick validation
select * from salesreport limit 100;
select * from prod_stock_inv limit 100;

//Validation
ALTER DYNAMIC TABLE prod_inv_alert REFRESH;
ALTER DYNAMIC TABLE prod_inv_alert SUSPEND;
ALTER DYNAMIC TABLE prod_inv_alert RESUME;

//Quick sanity check
select * from cumulative_purchase limit 10;
select * from cumulative_purchase where customer_id = 1652 order by running_total; 

-- check products with low inventory and alert
select * from prod_inv_alert where percent_unitleft < 10;



/*
7. Create email integration
*/
use role accountadmin;
--
//1.Create email integration
create notification integration if not exists notification_emailer
type=EMAIL
enabled=TRUE
allowed_recipients=('prakash.loganathaan@gmail.com')
comment='email integration to update on low product inventory levels'
;

//2.Granting the Privilege to Use the Notification Integration
GRANT USAGE ON INTEGRATION notification_emailer TO ROLE IDENTIFIER('"SYSADMIN"');

//3.Grant the EXECUTE ALERT global privilege to that custom role.
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE IDENTIFIER('"SYSADMIN"');

//4. Specifying Timestamps Based on Alert Schedules
GRANT DATABASE ROLE SNOWFLAKE.ALERTTO ROLE IDENTIFIER('"SYSADMIN"');


/*
8. Create alerts: These alerts will only run if there is new data in the dynamic table (low inventory products). So, its super easy to manage and maintain alerts in Snowflake on live data.
*/
use role sysadmin;
use warehouse compute_wh;
use schema demo_db.dt_demo;

create or replace alert alert_low_inv
warehouse = "COMPUTE_WH"
schedule = '1 MINUTE'
if (exists(select * from prod_inv_alert 
           WHERE percent_unitleft < 10 and ROWCREATIONTIME > SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
))
then call system$send_email(
 'notification_emailer' -- notification integration to use
,'prakash.loganathaan@gmail.com' --email
,'Email Alert: Low Inventory of products' --subject
,'Inventory running low for certain products. Please check the inventory report in snowflake table prod_inv_alert' --body of email
)
;

//Alerts are pause by default, so let's resume it first
-- Suspend Alerts 
-- Important step to suspend alert and stop consuming the warehouse credit
alter alert alert_low_inv RESUME;
alter alert alert_low_inv SUSPEND;

//Viewing Details About an Alert
SHOW ALERTS IN SCHEMA "DEMO_DB"."DT_DEMO";
DESC ALERT alert_low_inv;
SHOW INTEGRATIONS;

//Monitoring the Execution of Alerts
SELECT * FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(SCHEDULED_TIME_RANGE_START=>dateadd('day',-1,current_timestamp()), alert_name=>'alert_low_inv'))
ORDER BY SCHEDULED_TIME DESC;

-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));


/*
9. You can also monitor Dynamic Tables using the DYNAMIC_TABLE_REFRESH_HISTORY() function in INFORMATION_SCHEMA. This is sample SQL for dynamic tables in our data pipeline
*/
--https://docs.snowflake.com/en/sql-reference/functions/dynamic_table_refresh_history
select * from table(information_schema.dynamic_table_refresh_history())
where name in ('SALESREPORT','CUSTOMER_SALES_DATA_HISTORY','PROD_INV_ALERT','CUMULATIVE_PURCHASE')
--AND REFRESH_ACTION != 'NO_DATA'
order by DATA_TIMESTAMP DESC, REFRESH_END_TIME DESC LIMIT 10;

--https://docs.snowflake.com/en/sql-reference/functions/dynamic_table_graph_history
select * from table(information_schema.dynamic_table_graph_history())
where name in ('SALESREPORT','CUSTOMER_SALES_DATA_HISTORY','PROD_INV_ALERT','CUMULATIVE_PURCHASE')
;

/*
10. SUSPEND and RESUME Dynamic Tables
*/
-- Resume the data pipeline
alter dynamic table customer_sales_data_history RESUME;
alter dynamic table salesreport RESUME;
alter dynamic table prod_inv_alert RESUME;

-- Suspend the data pipeline
alter dynamic table customer_sales_data_history SUSPEND;
alter dynamic table salesreport SUSPEND;
alter dynamic table prod_inv_alert SUSPEND;

