//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
//System Functions
--https://docs.snowflake.com/en/sql-reference/functions-system

//SYSTEM$ALLOWLIST: Returns hostnames and port numbers to add to your firewall’s allowed list so that you can access Snowflake from behind your firewall. 
--https://docs.snowflake.com/en/sql-reference/functions/system_allowlist
select SYSTEM$ALLOWLIST() as allowlist;

//PARSE_JSON: Interprets an input string as a JSON document
--https://docs.snowflake.com/en/sql-reference/functions/parse_json
select parse_json(SYSTEM$ALLOWLIST()) as allowlist;

//Table Functions
--https://docs.snowflake.com/en/sql-reference/functions-table

//FLATTEN can be used to convert semi-structured data to a relational representation.
--https://docs.snowflake.com/en/sql-reference/functions/flatten
select
*
from table(flatten(input => parse_json(SYSTEM$ALLOWLIST())))
;

select
 VALUE:host::string as host
,VALUE:port::int as port
,VALUE:type::string as type
from table(flatten(input => parse_json(SYSTEM$ALLOWLIST())))
;
































/*------------------------------------------------------------------------------------*/

select parse_json(COLUMN1) from
values ('{
    "device_type": "cell_phone",
    "events": [
      {
        "f": 79,
        "rv": "786954.67,492.68,3577.48,40.11,343.00,345.8,0.22,8765.22",
        "t": 5769784730576,
        "v": {
          "ACHZ": 75846,
          "ACV": 098355,
          "DCA": 789,
          "DCV": 62287,
          "ENJR": 2234,
          "ERRS": 578,
          "MXEC": 999,
          "TMPI": 9
        },
        "vd": 54,
        "z": 1437644222811
      }
    ],
    "version": 3.2
  }');