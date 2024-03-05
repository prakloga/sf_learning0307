//Set worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database demo_db;
use schema public;
/*------------------------------------------------------------------------------------*/
//Create named external stage
/*Badge 1: Data Warehousing Workshop files
  We set up an S3 bucket on AWS in their US-West-2 Region. We named it "uni-lab-files." 
  Here is a link to the bucket: https://uni-lab-files.s3.us-west-2.amazonaws.com/
*/
create stage if not exists demo_db.public.sfdww_ext_stg
URL = 's3://uni-lab-files'
;

--Validation
show stages in database demo_db;
--
list @sfdww_ext_stg/;




create stage if not exists demo_db.public.sfdngw_ext_stg
URL = 's3://uni-kishore'
;

--Validation
show stages in database demo_db;
--
list @sfdngw_ext_stg/;




create stage if not exists demo_db.public.sfdlkw_ext_stg
URL = 's3://uni-klaus'
;

--Validation
show stages in database demo_db;
--
list @sfdlkw_ext_stg/;


CREATE OR REPLACE STAGE demo_db.public.sfquickstarts_ext_stg
    URL = 's3://sfquickstarts/'
;

--Validation
show stages in database demo_db;
--
list @sfquickstarts_ext_stg/;


list @snowpark_db.public.sf_int_stg/;
--rm @snowpark_db.public.sf_int_stg/q_history.parquet/;