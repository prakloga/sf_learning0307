//Lesson 8: Auto Data Unlimited 
/*
🎯 Create a Snowflake Account for Max at Auto Data Unlimited
During this workshop, you've been working in your Trial Account and in the ACME Snowflake Account you created. Now you will return to your Snowflake Trial account and set up a 3rd Account. 

This one you will use to pretend to be Max Manuf at Auto Data Unlimited (ADU).  This account will also give you a chance to try out Snowflake on Google Cloud Platform. 

NOTE: As with your ACME Account on AZURE, It's REALLY important that you use the CORRECT CLOUD and NAME when creating this account for MAX!! Name it AUTO_DATA_UNLIMITED and make sure it is on the Google Cloud Platform!!


🥋 Use ORGADMIN in WDE to Enable ORGADMIN in ADU
After creating the account for ADU, enable the ORGADMIN role on the account. You'll need it so that Max can share with Lottie/ACME.

NOTE: To enable ORGADMIN for ADU, you need to be in the ORGADMIN role within WDE. Set your role to ORGADMIN before completing the steps below. 

📓 The ADU VIN Data Infrastructure
Max set up his VIN Decoder infrastructure a few months ago when Auto Data Unlimited first moved to Snowflake.

His system takes in his customers' VINS, enhances each record with information about each vehicle and returns the enhanced data to the customer. 

By running the code in this lesson, you can emulate Max and his data infrastructure. We'll give you the code in large blocks. You will need to make sure you run and understand each command. 

🎯 Set Up the ADU Profile 
Navigate to the new ADU Account you created and set things up.

Create an XS Warehouse in the ADU Account. (Name it whatever you want)
Make sure SYSADMIN owns the Warehouse.
Set up the USER Profile on the ADU Account so that it is easy to recognize and keep separate from your ACME and WDE Accounts. 
You can use this graphic as the profile picture. 

Set your user's default warehouse. 
Enable Email Notifications in the Profile.
Create a Resource Monitor that limits the account to 1 credit per day.  (Name it whatever you want but the name can't start with a number).

🎯 Set Up Max's Data Infrastructure 
Everything you create in the ADU account should be owned by SYSADMIN. If you create something as ACCOUNTADMIN, switch the ownership to SYSADMIN. 

Create a new database and name it VIN.
Drop the PUBLIC schema and create a new schema called DECODE.
Check to make sure the database and schema are owned by SYSADMIN. 

*/
