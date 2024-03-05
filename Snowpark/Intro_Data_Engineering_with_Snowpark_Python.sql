-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects (ACCOUNTADMIN part)
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
-- Roles
SET MY_USER = CURRENT_USER();
SELECT $MY_USER;
--
CREATE OR REPLACE ROLE HOL_ROLE;
GRANT ROLE HOL_ROLE TO ROLE SYSADMIN;
GRANT ROLE HOL_ROLE TO USER IDENTIFIER($MY_USER);
--
GRANT EXECUTE TASK ON ACCOUNT TO ROLE HOL_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE HOL_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE HOL_ROLE;

-- Databases
CREATE OR REPLACE DATABASE HOL_DB;
GRANT OWNERSHIP ON DATABASE HOL_DB TO ROLE HOL_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE HOL_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE HOL_WH TO ROLE HOL_ROLE;

-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;
USE SCHEMA HOL_SCHEMA;

-- Schemas
CREATE OR REPLACE SCHEMA HOL_SCHEMA;

-- External Frostbyte objects
USE SCHEMA HOL_SCHEMA;
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;

-- list stage objects
LIST @FROSTBYTE_RAW_STAGE;

-- ----------------------------------------------------------------------------
-- Step #1: Connect to weather data in Marketplace
-- ----------------------------------------------------------------------------

/*---
But what about data that needs constant updating - like the WEATHER data? We would
need to build a pipeline process to constantly update that data to keep it fresh.

Perhaps a better way to get this external data would be to source it from a trusted
data supplier. Let them manage the data, keeping it accurate and up to date.

Enter the Snowflake Data Cloud...

Weather Source is a leading provider of global weather and climate data and their
OnPoint Product Suite provides businesses with the necessary weather and climate data
to quickly generate meaningful and actionable insights for a wide range of use cases
across industries. Let's connect to the "Weather Source LLC: frostbyte" feed from
Weather Source in the Snowflake Data Marketplace by following these steps:

    -> Snowsight Home Button
         -> Marketplace
             -> Search: "Weather Source LLC: frostbyte" (and click on tile in results)
                 -> Click the blue "Get" button
                     -> Under "Options", adjust the Database name to read "FROSTBYTE_WEATHERSOURCE" (all capital letters)
                        -> Grant to "HOL_ROLE"
    
That's it... we don't have to do anything from here to keep this data updated.
The provider will do that for us and data sharing means we are always seeing
whatever they they have published.


-- You can also do it via code if you know the account/share details...
SET WEATHERSOURCE_ACCT_NAME = '*** PUT ACCOUNT NAME HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE_NAME = '*** PUT ACCOUNT SHARE HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE = $WEATHERSOURCE_ACCT_NAME || '.' || $WEATHERSOURCE_SHARE_NAME;

CREATE OR REPLACE DATABASE FROSTBYTE_WEATHERSOURCE
  FROM SHARE IDENTIFIER($WEATHERSOURCE_SHARE);

GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE TO ROLE HOL_ROLE;
---*/


-- Let's look at the data - same 3-part naming convention as any other table
SHOW VIEWS IN SCHEMA FROSTBYTE_WEATHERSOURCE.ONPOINT_ID;
SELECT * FROM FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.POSTAL_CODES LIMIT 100;

-- ----------------------------------------------------------------------------
-- Step 1: Inspect the Excel files
-- ----------------------------------------------------------------------------

-- Make sure the two Excel files show up in the stage
LIST @FROSTBYTE_RAW_STAGE/intro;

-- I've also included a copy of the Excel files in the /data folder of this repo
-- so you can look at the contents.


-- ----------------------------------------------------------------------------
-- Step 2: Create the stored procedure to load Excel files
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(file_path string, worksheet_name string, target_table string)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'openpyxl')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd
 
def main(session, file_path, worksheet_name, target_table):
 with SnowflakeFile.open(file_path, 'rb') as f:
     workbook = load_workbook(f)
     sheet = workbook.get_sheet_by_name(worksheet_name)
     data = sheet.values
 
     # Get the first line in file as a header line
     columns = next(data)[0:]
     # Create a DataFrame based on the second and subsequent lines of data
     df = pd.DataFrame(data, columns=columns)
 
     df2 = session.create_dataframe(df)
     df2.write.mode("overwrite").save_as_table(target_table)
 
 return True
$$;


-- ----------------------------------------------------------------------------
-- Step 3: Load the Excel data
-- ----------------------------------------------------------------------------

CALL LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@FROSTBYTE_RAW_STAGE, 'intro/order_detail.xlsx'), 'order_detail', 'ORDER_DETAIL');
CALL LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@FROSTBYTE_RAW_STAGE, 'intro/location.xlsx'), 'location', 'LOCATION');

DESCRIBE TABLE ORDER_DETAIL;
SELECT * FROM ORDER_DETAIL;

DESCRIBE TABLE LOCATION;
SELECT * FROM LOCATION;


-- ----------------------------------------------------------------------------
-- Step 1: Create the stored procedure to load DAILY_CITY_METRICS
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE LOAD_DAILY_CITY_METRICS_SP()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{}' AND TABLE_NAME='{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def main(session: Session) -> str:
    schema_name = "HOL_SCHEMA"
    table_name = "DAILY_CITY_METRICS"

    # Define the tables
    order_detail = session.table("ORDER_DETAIL")
    location = session.table("LOCATION")
    history_day = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY")

    # Join the tables
    # https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.functions.builtin
    order_detail = order_detail.join(location, order_detail["LOCATION_ID"] == location["LOCATION_ID"])
    order_detail = order_detail.join(history_day, (((F.call_function("DATE", order_detail["ORDER_TS"])) == history_day["DATE_VALID_STD"]) & (location["ISO_COUNTRY_CODE"] == history_day["COUNTRY"]) & (location["CITY"] == history_day["CITY_NAME"])))

    # Aggregate the data
    final_agg = (order_detail.group_by(F.col("DATE_VALID_STD")
                                      ,F.col("CITY_NAME")
                                      ,F.col("ISO_COUNTRY_CODE"))
                             .agg(F.sum("PRICE").alias("DAILY_SALES_SUM")
                                 ,F.avg("AVG_TEMPERATURE_AIR_2M_F").alias("AVG_TEMPERATURE_F")
                                 ,F.avg("TOT_PRECIPITATION_IN").alias("AVG_PRECIPITATION_IN"))
                             .select(F.col("DATE_VALID_STD").alias("DATE")
                                    ,F.col("CITY_NAME")
                                    ,F.col("ISO_COUNTRY_CODE").alias("COUNTRY_DESC")
                                    # https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
                                    ,F.call_function("ZEROIFNULL", F.col("DAILY_SALES_SUM")).alias("DAILY_SALES")
                                    # https://docs.snowflake.com/en/sql-reference/functions/round
                                    ,F.round(F.col("AVG_TEMPERATURE_F"), 2).alias("AVG_TEMPERATURE_FAHRENHEIT")
                                    ,F.round(F.col("AVG_PRECIPITATION_IN"), 2).alias("AVG_PRECIPITATION_INCHES"))
                )

    # If the table doesn't exist then create it
    if not table_exists(session, schema=schema_name, name=table_name):
        # https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.DataFrame.write
        final_agg.write.mode("overwrite").save_as_table(table_name)

        return f"Successfully created {table_name}"

    # Otherwise update it
    else:
        cols_to_update = {c: final_agg[c] for c in final_agg.schema.names}
        '''
        {'DATE': Column("DATE"),
        'CITY_NAME': Column("CITY_NAME"),
        'COUNTRY_DESC': Column("COUNTRY_DESC"),
        'DAILY_SALES': Column("DAILY_SALES"),
        'AVG_TEMPERATURE_FAHRENHEIT': Column("AVG_TEMPERATURE_FAHRENHEIT"),
        'AVG_PRECIPITATION_INCHES': Column("AVG_PRECIPITATION_INCHES")}
        '''

        dcm = session.table(f"{schema_name}.{table_name}")
        # https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.Table.merge
        (dcm.merge(final_agg, (dcm["DATE"] == final_agg["DATE"]) & (dcm["CITY_NAME"] == final_agg["CITY_NAME"]) & (dcm["COUNTRY_DESC"] == final_agg["COUNTRY_DESC"])
                            , [F.when_matched().update(cols_to_update), F.when_not_matched().insert(cols_to_update)])
        )

        return f"Successfully updated {table_name}"
$$
;

--OR--

CREATE OR REPLACE PROCEDURE LOAD_DAILY_CITY_METRICS_SP()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def main(session: Session) -> str:
    schema_name = "HOL_SCHEMA"
    table_name = "DAILY_CITY_METRICS"

    # Define the tables
    order_detail = session.table("ORDER_DETAIL")
    history_day = session.table("FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY")
    location = session.table("LOCATION")

    # Join the tables
    order_detail = order_detail.join(location, order_detail['LOCATION_ID'] == location['LOCATION_ID'])
    order_detail = order_detail.join(history_day, (F.builtin("DATE")(order_detail['ORDER_TS']) == history_day['DATE_VALID_STD']) & (location['ISO_COUNTRY_CODE'] == history_day['COUNTRY']) & (location['CITY'] == history_day['CITY_NAME']))

    # Aggregate the data
    final_agg = order_detail.group_by(F.col('DATE_VALID_STD'), F.col('CITY_NAME'), F.col('ISO_COUNTRY_CODE')) \
                        .agg( \
                            F.sum('PRICE').alias('DAILY_SALES_SUM'), \
                            F.avg('AVG_TEMPERATURE_AIR_2M_F').alias("AVG_TEMPERATURE_F"), \
                            F.avg("TOT_PRECIPITATION_IN").alias("AVG_PRECIPITATION_IN"), \
                        ) \
                        .select(F.col("DATE_VALID_STD").alias("DATE"), F.col("CITY_NAME"), F.col("ISO_COUNTRY_CODE").alias("COUNTRY_DESC"), \
                            F.builtin("ZEROIFNULL")(F.col("DAILY_SALES_SUM")).alias("DAILY_SALES"), \
                            F.round(F.col("AVG_TEMPERATURE_F"), 2).alias("AVG_TEMPERATURE_FAHRENHEIT"), \
                            F.round(F.col("AVG_PRECIPITATION_IN"), 2).alias("AVG_PRECIPITATION_INCHES"), \
                        )

    # If the table doesn't exist then create it
    if not table_exists(session, schema=schema_name, name=table_name):
        final_agg.write.mode("overwrite").save_as_table(table_name)

        return f"Successfully created {table_name}"
    # Otherwise update it
    else:
        cols_to_update = {c: final_agg[c] for c in final_agg.schema.names}

        dcm = session.table(f"{schema_name}.{table_name}")
        dcm.merge(final_agg, (dcm['DATE'] == final_agg['DATE']) & (dcm['CITY_NAME'] == final_agg['CITY_NAME']) & (dcm['COUNTRY_DESC'] == final_agg['COUNTRY_DESC']), \
                            [F.when_matched().update(cols_to_update), F.when_not_matched().insert(cols_to_update)])

        return f"Successfully updated {table_name}"
$$;

-- ----------------------------------------------------------------------------
-- Step 2: Load the DAILY_CITY_METRICS table
-- ----------------------------------------------------------------------------

CALL LOAD_DAILY_CITY_METRICS_SP();

SELECT * FROM DAILY_CITY_METRICS;

