use role sysadmin;
--use warehouse compute_wh;
use warehouse intl_wh;
use schema intl_db.public;
/*---------------------------------------------------------*/
//Lesson 4: Sharing Data with Other Accounts

//🖼️ A Snowflake Account for ACME

//📓 Set Up an Account for ACME
/*
You are already using your trial to simulate a World Data Emporium Snowflake Account.
Now we want you to simulate ACME's account, so you'll need a second account.
In the real world, ACME would set up their own account or just enter billing details for their existing trial, but this is a good opportunity for you to see how easy it is to create accounts using the ORGADMIN role. So that's what you're going to do next. 
*/
//🥋 Create an Azure Snowflake Account for ACME
//📓 Switching Between Accounts
//🥋 Sign In to the ACME Account and Update Your Password
//🎯 CHALLENGE LAB: Set Up the ACME Account
/*
Create a warehouse called ACME_WH, make it size XS. 
Update the USER profile by setting the image, name, default ROLE and default WAREHOUSE. 
The warehouse should be owned by the SYSADMIN ROLE. 
The default ROLE should be SYSADMIN. 
The Profile Image and name should look like the image above. 
*/

//🥋 Martín Sets Up World Data Emporium as a Listing Provider
//🥋 Martín Creates a Listing
/*
NOTE: Before creating the share, make sure all objects in your INTL_DB are owned by SYSADMIN. Once you add an object to a share it becomes harder to transfer ownership. 
In that case, you have to remove the object from the share it, transfer ownership on the object, and then add it back to the share. So do yourself a favor and take care of it before creating the share.
*/


//🥋 "Get" the WDE Listing
//📓 How Sharing Will Work
/*
Because your trial account (WDE) and your ACME account are on different clouds, in different regions, Snowflake will automatically perform replication to the other cloud/region.
This will be a cost that WDE/Osiris covers. If Osiris doesn't want to cover that cost, he could insist that Lottie's team get a Snowflake account on the same cloud and in the same region as his primary account. This may become part of their negotiations.
If we look at the WDE Account (your original trial account) we see that a new database with an odd name has been created. 

We won't delve into the $GDS database and how it works during this workshop.  Just know that Snowflake will manage the replication on behalf of the provider, and this database is a reminder of that. Feel free to navigate into that database and have a look around. 
*/

//📓 What is a Listing?
//🥋 Add a Data Dictionary to the COUNTRY_CODE_TO_CURRENCY_CODE Table.
//🥋 Add A Sample Query

//🥋 View the Listing in the ACME Account
//🎯 Get the Listing for the ACME Account
//🥋 Convert "Regular" Views to Secure Views
alter view INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO set secure;
alter view INTL_DB.PUBLIC.SIMPLE_CURRENCY set secure;

//🥋 Add the Newly Secure Views to Your Outbound Share

//🔍 Did You Notice? 
--When you were creating your share, the two SNOWFLAKE_SAMPLE_DATA database and the SNOWFLAKE database didn't show up as options. 

//🔍 Was that Odd?
--Initially, neither of the views showed up, but then, after some changes, they both showed up. 

//🔍 Still a No Go?
--After the views showed up, only one could be added to our share.  

//🎯 Navigate to Your Usage Page Yet Again!
--This time, take a look at the drop menus at the top and the options that are available in those drop menus.

//🎯 Exploring Usage Types
/*
The menu that starts out saying [All Usage Types] can cause other menu options to appear after it depending on which Usage Type you select. Choose the Data Transfer option and then notice that there are 3 colors that represent 3 different transfer types. Purple represents the running of External Functions. The DORA GRADER is an External Function and so is the GREETING Function. So, the purple bar represents the amount of data you have transfered. 

45kB is a small amount of data -- less than one page of text in a Google Sheets document. 
*/

//🎯 Find Your Way to the Snowflake App/Database Organization Usage Schema










