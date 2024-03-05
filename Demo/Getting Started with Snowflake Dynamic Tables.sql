//https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/index.html#1

/*
We are going to build our lab in a database called "demo" and schema name "dt_demo". 
Feel free to use any database if "demo" database is already in use or you don't have access to it.
*/
CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.DT_DEMO;
USE SCHEMA DEMO.DT_DEMO;

/*
Once the database is created, we will create 3 UDTF to generate our source data. 
First table is CUST_INFO and insert 1000 customers into it using this new Python UDTF.
*/
create or replace function gen_cust_info(num_records number)
returns table (custid number(10), cname varchar(100), spendlimit number(10,2))
language python
runtime_version=3.8
handler='CustTab'
packages = ('Faker')
as $$
from faker import Faker
import random

fake = Faker()
# Generate a list of customers  

class CustTab:
    # Generate multiple customer records
    def process(self, num_records):
        customer_id = 1000 # Starting customer ID                 
        for _ in range(num_records):
            custid = customer_id + 1
            cname = fake.name()
            spendlimit = round(random.uniform(1000, 10000),2)
            customer_id += 1
            yield (custid,cname,spendlimit)

$$;

create or replace table cust_info as select * from table(gen_cust_info(1000)) order by 1;
select * from cust_info;

//Next table is PROD_STOCK_INV and insert 100 products inventory into it using this new Python UDTF.
create or replace function gen_prod_inv(num_records number)
returns table (pid number(10), pname varchar(100), stock number(10,2), stockdate date)
language python
runtime_version=3.8
handler='ProdTab'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta
fake = Faker()

class ProdTab:
    # Generate multiple product records
    def process(self, num_records):
        product_id = 100 # Starting customer ID                 
        for _ in range(num_records):
            pid = product_id + 1
            pname = fake.catch_phrase()
            stock = round(random.uniform(500, 1000),0)
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (3 months from now)
            min_date = current_date - timedelta(days=90)
            
            # Generate a random date within the date range
            stockdate = fake.date_between_dates(min_date,current_date)

            product_id += 1
            yield (pid,pname,stock,stockdate)

$$;

create or replace table prod_stock_inv as select * from table(gen_prod_inv(100)) order by 1;
select * from prod_stock_inv;

//Next table is SALESDATA to store raw product sales by customer and purchase date
create or replace function gen_cust_purchase(num_records number,ndays number)
returns table (custid number(10), purchase variant)
language python
runtime_version=3.8
handler='genCustPurchase'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    # Generate multiple customer purchase records
    def process(self, num_records,ndays):       
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)
            
            #print(c_id)
            customer_purchase = {
                'custid': c_id,
                'purchased': []
            }
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (days from now)
            min_date = current_date - timedelta(days=ndays)
            
            # Generate a random date within the date range
            pdate = fake.date_between_dates(min_date,current_date)
            
            purchase = {
                'prodid': fake.random_int(min=101, max=199),
                'quantity': fake.random_int(min=1, max=5),
                'purchase_amount': round(random.uniform(10, 1000),2),
                'purchase_date': pdate
            }
            customer_purchase['purchased'].append(purchase)
            
            #customer_purchases.append(customer_purchase)
            yield (c_id,purchase)

$$;

-- Create table and insert records 
create or replace table salesdata as select * from table(gen_cust_purchase(10000,10));
select * from salesdata;

//Check if there is data in all 3 raw tables -
-- customer information table, each customer has spending limits
select * from cust_info limit 10;

-- product stock table, each product has stock level from fulfilment day
select * from prod_stock_inv limit 10;

-- sales data for products purchsaed online by various customers
select * from salesdata limit 10;

/*
For this we will extract the sales information from the salesdata table and join it with customer information to build the customer_sales_data_history, note that we are extracting raw json data(schema on read) and transforming it into meaningful columns and data type
*/

create or replace dynamic table customer_sales_data_history
target_lag = 'DOWNSTREAM'
warehouse = 'COMPUTE_WH'
as
select
 s.custid as customer_id
,c.cname as customer_name
,s.PURCHASE:prodid::number(5) as product_id
,s.PURCHASE:purchase_amount::number(10) as saleprice
,s.PURCHASE:quantity::number(5) as quantity
,s.PURCHASE:purchase_date::date as salesdate
from cust_info as c
join salesdata as s
on c.custid = s.custid
;

-- quick sanity check
select * from customer_sales_data_history limit 10;
select count(*) from customer_sales_data_history;

/*
Now, let's combine these results with the product table and create a SCD TYPE 2 transformation using window the function "LEAD", 
it gives us the subsequent rows in the same result set to build a TYPE 2 transformation.
*/
create or replace dynamic table salesreport
target_lag = '1 minute'
warehouse = 'COMPUTE_WH'
as
select
 t1.CUSTOMER_ID
,t1.CUSTOMER_NAME
,t1.PRODUCT_ID
,p.pname as product_name
,t1.saleprice
,t1.quantity
,(t1.saleprice/t1.quantity) as unitsalesprice
,t1.salesdate as creationtime
,t1.CUSTOMER_ID||'-'||t1.PRODUCT_ID||'-'||t1.salesdate as customer_sk
,lead(t1.salesdate) over(partition by t1.CUSTOMER_ID order by t1.salesdate asc) as  end_time
from customer_sales_data_history as t1
inner join prod_stock_inv as p
on t1.product_id = p.pid
--where t1.customer_id in (1087)
--order by t1.customer_id, t1.salesdate
;

//salesreport table should refresh automatically. Let's check the results.
select * from salesreport limit 10;
select count(*) from salesreport;

//Test
//Let's test this DAG by adding some raw data in the base tables.
-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));

-- Check raw base table
select count(*) from salesdata;

-- Check Dynamic Tables after a minute
select count(*) from customer_sales_data_history;
select count(*) from salesreport;








