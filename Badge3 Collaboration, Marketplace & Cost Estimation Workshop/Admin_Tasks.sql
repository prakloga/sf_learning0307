//Did you know you can run select current_account(); in a Snowflake worksheet and Snowflake will tell you your Account Locator? 
--Account Locator! Enter the 2-3 letters followed by the 5-6 numbers of your Account Locator
SELECT CURRENT_ACCOUNT(), CURRENT_REGION(); --LWB18040, AWS_US_EAST_1

//📓 Budgets! New in June 2023
use role accountadmin;
call snowflake.local.account_root_budget!ACTIVATE();

use role orgadmin;
create ACCOUNT IDENTIFIER('"ACME"') ADMIN_NAME = 'ACME_ADMIN' ADMIN_PASSWORD = '☺☺☺☺☺☺☺☺☺' EMAIL = 'prakash.loganathaan@gmail.com' EDITION = 'ENTERPRISE' REGION = 'AZURE_EASTUS2' REGION_GROUP = 'PUBLIC';

create ACCOUNT IDENTIFIER('"AUTO_DATA_UNLIMITED"') ADMIN_NAME = 'ADU_ADMIN' ADMIN_PASSWORD = '☺☺☺☺☺☺☺☺' EMAIL = 'prakash.loganathaan@gmail.com' EDITION = 'ENTERPRISE' REGION = 'GCP_US_EAST4' REGION_GROUP = 'PUBLIC';

alter ACCOUNT IDENTIFIER('"AUTO_DATA_UNLIMITED"') set IS_ORG_ADMIN = true;






