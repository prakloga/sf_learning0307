use role sysadmin;
/*
https://quickstarts.snowflake.com/guide/tasty_bytes_introduction/#0
-----
Global food truck:
 - Localized menu options
 - 15 countries
 - 30 major cities
 - 15 core brands
 -----
 Location Served:
 1.USA          : San Mateo, Denver, Seattle, Boston, New York City
 2.Canada       : Toronto, Vancouver, Montreal
 3.United Kindom: London, Manchester
 4.France       : Paris, Nice
 5.Poland       : Warsaw, Krakow
 6.India        : Mumbai, Delhi
 7.Japan        : Tokyo
 8.South Korea  : Seoul
 9.Australia    : Sydney, Melbourne

 Current state and future goals:
 Trucks: Current: 450 | Future: 1120
 Sales: $105M/year | $320M/year
 NPS: 3 | 40
 
 Roles:
  - tasty_admin
  - tasty_data_engineer
  - tasty_data_scientist
  - tasty_bi
  - tasty_data_app
  - tasty_dev

 Warehouse:
  - demo_build_wh     --> sysadmin
  - tasty_de_wh       --> tasty_admin & tasty_data_engineer
  - tasty_ds_wh       --> tasty_admin & tasty_data_scientist
  - tasty_bi_wh       --> tasty_admin & tasty_bi
  - tasty_dev_wh      --> tasty_admin & tasty_data_engineer & tasty_dev
  - tasty_data_app_wh --> tasty_admin & tasty_data_app
 
 Database:
  - frostbyte_tasty_bytes

 Schema:
  - raw_pos
  - raw_customer
  - harmonized
  - analytics

 File Formats:
  - frostbyte_tasty_bytes.public.csv_ff

 External Stage:
  - frostbyte_tasty_bytes.public.s3load
    - @frostbyte_tasty_bytes.public.s3load/raw_pos/country/
    - @frostbyte_tasty_bytes.public.s3load/raw_pos/franchise/
    - @frostbyte_tasty_bytes.public.s3load/raw_pos/location/
    - @frostbyte_tasty_bytes.public.s3load/raw_pos/menu/
    - @frostbyte_tasty_bytes.public.s3load/raw_pos/truck/
    - @frostbyte_tasty_bytes.public.s3load/raw_customer/customer_loyalty/
    - @frostbyte_tasty_bytes.public.s3load/raw_pos/order_header/
    - @frostbyte_tasty_bytes.public.s3load/raw_pos/order_detail/

 Tables:
  - frostbyte_tasty_bytes.raw_pos.country
  - frostbyte_tasty_bytes.raw_pos.franchise
  - frostbyte_tasty_bytes.raw_pos.location
  - frostbyte_tasty_bytes.raw_pos.menu
  - frostbyte_tasty_bytes.raw_pos.truck
  - frostbyte_tasty_bytes.raw_pos.order_header
  - frostbyte_tasty_bytes.raw_pos.order_detail
  - frostbyte_tasty_bytes.raw_customer.customer_loyalty

Views:
  - frostbyte_tasty_bytes.harmonized.orders_v
  - frostbyte_tasty_bytes.harmonized.customer_loyalty_metrics_v
  - frostbyte_tasty_bytes.analytics.orders_v
  - frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v
  

 
 */