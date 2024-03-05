//set worksheet context
use role sysadmin;
use warehouse compute_wh;
use schema demo_db.window_func;
/*-----------------------------------------------------------------------------*/
select * from sales_table;

--https://coffingdw.com/snowflake-analytics-part-1/
/*
In each example, you will see an ORDER BY statement, but it will not come at the end of the query. The ORDER BY keywords is always within the analytic calculation. These analytics will take the data set and sort it with an ORDER BY statement.  Once the data sort happens in step 1 of the process, the calculations begin.

In its most simple explanation, a ROW_NUMBER will sort the data first with an ORDER BY statement and then place the number 1 on the first row, a number 2 on the second row, and so on.  Check out the example below.
*/
select
 product_id
,sale_date
,daily_sales
,row_number() over(order by product_id, sale_date) as seq_number
from sales_table
;

/*
Each analytic example will have an ORDER BY statement, but sometimes you will also have a PARTITION statement.  If you see the keyword PARTITION, it means the analytic calculation will reset and start over.  Check out the next example below.
*/
select
 product_id
,sale_date
,daily_sales
,row_number() over(partition by product_id order by product_id, sale_date) as seq_number
from sales_table
;


select
*
from student_table
;