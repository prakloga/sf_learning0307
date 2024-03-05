//Delegating Warehouse Management
//https://docs.snowflake.com/en/user-guide/warehouses-tasks
use role accountadmin;

--Create a new role that will create and own a new warehouse, and grant the CREATE WAREHOUSE privilege to that role:
create role create_wh_role comment="Test";
grant create warehouse on account to role create_wh_role;
grant role create_wh_role to role sysadmin;

--Create a second role that will manage all warehouses in the account, and grant the MANAGE WAREHOUSES privilege to that role:
create or replace role manage_wh_role comment='Test';
grant manage warehouses on account to role manage_wh_role;
grant role manage_wh_role to role sysadmin;

--Using the create_wh_role role, create a new warehouse:
use role create_wh_role;

create or replace warehouse test_std_wh
with warehouse_type = standard
warehouse_size = xsmall
max_cluster_count = 2
min_cluster_count = 1
scaling_policy = standard
auto_suspend = 60
auto_resume = true
initially_suspended = true
enable_query_acceleration = true
query_acceleration_max_scale_factor = 0
comment = 'test'
;

create or replace warehouse test_spo_wh
with warehouse_type = 'snowpark-optimized'
warehouse_size = medium
max_cluster_count = 2
min_cluster_count = 1
scaling_policy = standard
auto_suspend = 60
auto_resume = true
initially_suspended = true
enable_query_acceleration = true
query_acceleration_max_scale_factor = 0
comment = 'test'
;

show warehouses like 'test_%' in account;


//Change the current role to manage_wh_role:
use role manage_wh_role;
alter warehouse test_wh suspend;
alter warehouse test_wh resume;
alter warehouse test_wh set warehouse_size = SMALL;
desc warehouse test_wh;

//Identifying Queries with the SYSTEM$ESTIMATE_QUERY_ACCELERATION Function
select parse_json(system$estimate_query_acceleration('<query id>'));

//Identifying Queries and Warehouses with the QUERY_ACCELERATION_ELIGIBLE View
use role sysadmin;
use warehouse compute_wh;

select * from snowflake.account_usage.query_acceleration_eligible limit 10;
select * from snowflake.account_usage.query_acceleration_history limit 10;
select * from table(information_schema.query_acceleration_history());






