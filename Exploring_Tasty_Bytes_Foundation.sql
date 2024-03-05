//Step 1 - Exploring the Tasty Bytes Database
use role sysadmin;
show databases like 'frostbyte_tasty_bytes';
//Step 2 - Exploring the Schemas within the Tasty Bytes Database
show schemas in database frostbyte_tasty_bytes;
//Step 3 - Exploring the Tables within the RAW_POS Schema within the Tasty Bytes Database
show tables in schema frostbyte_tasty_bytes.raw_pos;
show tables in database frostbyte_tasty_bytes;
//Step 4 - Exploring the Tasty Bytes Roles
show roles like 'tasty%';
//Step 5 - Exploring the Tasty Bytes Warehouses
show warehouses like 'tasty%';
//Step 6 - Putting it All Together
/*
1.Assume the tasty_data_engineer role via USE ROLE
2.Leverage the tasty_de_wh Warehouse via USE WAREHOUSE
3.Query our raw_pos.menu table to find which Menu Items are sold at our Plant Palace branded food trucks.
*/
use role tasty_data_engineer;
use warehouse tasty_de_wh;
use database frostbyte_tasty_bytes;
select
    *
from
    raw_pos.menu
limit
    250;
select
    *
from
    raw_pos.menu
where
    truck_brand_name = 'Plant Palace'
limit
    250;
select
    distinct MENU_ITEM_NAME
from
    raw_pos.menu
where
    truck_brand_name = 'Plant Palace';
select
    menu_type_id,
    menu_type,
    truck_brand_name,
    menu_item_name
from
    raw_pos.menu
where
    truck_brand_name = 'Plant Palace';
//4. Powered by Tasty Bytes - Quickstarts
    --https://quickstarts.snowflake.com/guide/tasty_bytes_introduction/index.html?index=..%2F..index#3
    CREATE
    OR REPLACE RESOURCE MONITOR tasty_test_rm WITH CREDIT_QUOTA = 100 -- 100 credits
    FREQUENCY = monthly -- reset the monitor monthly
    START_TIMESTAMP = immediately -- begin tracking immediately
    TRIGGERS ON 75 PERCENT DO NOTIFY -- notify accountadmins at 75%
    ON 100 PERCENT DO SUSPEND -- suspend warehouse at 100 percent, let queries finish
    ON 110 PERCENT DO SUSPEND_IMMEDIATE;
-- suspend warehouse and cancel all queries at 110 percent
    //Create resource monitor
    use role accountadmin;
CREATE RESOURCE MONITOR IF NOT EXISTS IDENTIFIER('"TASTYBYTES_VWH_RM"') CREDIT_QUOTA = 100 -- 100 credits
    FREQUENCY = 'MONTHLY' -- reset the monitor monthly
    START_TIMESTAMP = 'IMMEDIATELY' -- begin tracking immediately
    TRIGGERS ON 100 PERCENT DO SUSPEND -- suspend warehouse at 100 percent, let queries finish
    ON 110 PERCENT DO SUSPEND_IMMEDIATE -- suspend warehouse and cancel all queries at 110 percent
    ON 75 PERCENT DO NOTIFY -- notify accountadmins at 75%
;
//Apply resource monitor to VWH
    use role accountadmin;
alter WAREHOUSE IDENTIFIER('"TASTY_BI_WH"')
set
    RESOURCE_MONITOR = 'TASTYBYTES_VWH_RM';
alter WAREHOUSE IDENTIFIER('"TASTY_DATA_APP_WH"')
set
    RESOURCE_MONITOR = 'TASTYBYTES_VWH_RM';
alter WAREHOUSE IDENTIFIER('"TASTY_DEV_WH"')
set
    RESOURCE_MONITOR = 'TASTYBYTES_VWH_RM';
alter WAREHOUSE IDENTIFIER('"TASTY_DE_WH"')
set
    RESOURCE_MONITOR = 'TASTYBYTES_VWH_RM';
alter WAREHOUSE IDENTIFIER('"TASTY_DS_WH"')
set
    RESOURCE_MONITOR = 'TASTYBYTES_VWH_RM';