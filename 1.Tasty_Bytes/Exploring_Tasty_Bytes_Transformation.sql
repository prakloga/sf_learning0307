//Step 1 - Create a Clone of Production
/*
Zero Copy Cloning: Creates a copy of a database, schema or table. A snapshot of data present in the source object is taken when the clone is created and is made available to the cloned object. The cloned object is writable and is independent of the clone source. That is, changes made to either the source object or the clone object are not part of the other.
*/
use role tasty_dev;
create or replace table frostbyte_tasty_bytes.raw_pos.truck_dev clone frostbyte_tasty_bytes.raw_pos.truck;

//Step 1 - Querying our Cloned Table
use warehouse tasty_dev_wh;
use database frostbyte_tasty_bytes;

select
t.truck_id
,t.year
,t.make
,t.model
from frostbyte_tasty_bytes.raw_pos.truck_dev as t
order by t.truck_id
;
