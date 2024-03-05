//Lesson 2: Reviewing Data Structuring & Stage Types
--Snowflake Stage Object: It's a named gateway that allows Snowflake users to easily connect to cloud folders and access the data stored in them.

//🥋 Klaus' Bucket
/*
We already know that Klaus has a bucket named "uni-klaus". We also know he made his bucket and the files in it publicly readable. 
To access that bucket using a web browser, we can hack together a URL. 

https://<bucket name>.s3.<region>.amazonaws.com/
https://uni-klaus.s3.us-west-2.amazonaws.com/
https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/90s_tracksuit.png

Klaus' bucket is in the us-west-2 region. Can you build a URL and view the XML list of files and folders in Klaus' bucket by opening it in a browser tab? 
Once you are able to view the XML list of folders and files in the bucket, answer the question below.
*/

//🥋 Create a Database for Zena's Athleisure Idea
/*
You will create an External Stage in Snowflake that points to the "clothing" folder in Klaus' bucket, but before you do that:
Create a database called ZENAS_ATHLEISURE_DB and make sure the SYSADMIN role owns it. 
Drop the PUBLIC schema
Create a schema called PRODUCTS (make sure it is also owned by SYSADMIN). 
*/
USE ROLE SYSADMIN;
--
CREATE DATABASE IF NOT EXISTS IDENTIFIER('"ZENAS_ATHLEISURE_DB"') COMMENT = 'Badge 4: Data Lake Workshop';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('"ZENAS_ATHLEISURE_DB"."PRODUCTS"') COMMENT = 'Badge 4: Data Lake Workshop';
DROP SCHEMA IF EXISTS IDENTIFIER('"ZENAS_ATHLEISURE_DB"."PUBLIC"');

//🥋 Create a Stage to Access the Sweat Suit Images
/*
Name the stage UNI_KLAUS_CLOTHING.
Figure out the URL for the bucket and enter it. Remember it starts with s3:// 
Make sure the URL includes /clothing -- Zena doesn't need or want access to all Klaus' files, just the ones in the clothing folder. 
After you create the stage, go to a worksheet and run a list command to see if you can see ONLY the files in the /clothing folder of Klaus' bucket! 
*/

//Create stage
CREATE OR REPLACE STAGE ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING
	URL = 's3://uni-klaus/clothing'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 4: Data Lake Workshop'
;

//List stage values
list @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING;


//🎯 Create another Stage for another of Klaus' folders!
/*
Again, in the PRODUCTS schema of Zena's database, and owned by SYSADMIN...
This time point the stage at the zenas_metadata folder in Klaus' bucket, and call it UNI_KLAUS_ZMD 
Then, run the list command and see how many files are in that folder!
*/
//Create stage
CREATE OR REPLACE STAGE ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD
	URL = 's3://uni-klaus/zenas_metadata'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 4: Data Lake Workshop'
;

//List stage values
list @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_ZMD;

//🎯 Create A 3rd Stage!
/*
Again, in the PRODUCTS schema of Zena's database, and owned by SYSADMIN...
This time point the stage at the remaining folder in Klaus' bucket. Call it UNI_KLAUS_SNEAKERS 
Then, run the list command and see how many files are in that folder!
*/
//Create stage
CREATE OR REPLACE STAGE ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_SNEAKERS
	URL = 's3://uni-klaus/sneakers'
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Badge 4: Data Lake Workshop'
;

//List stage values
list @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_SNEAKERS;

// 🏁 Ready to Mark Lessons 1 & 2 Complete? 
/*
If you can: 
1.Name the 3 structural data types Snowflake can manage: Structured, semi-structured, Unstructured
2.List the 5 semi-structured data types Snowflake can load into VARIANT columns in tables: JSON, Parquet, Avro, ORC, XML
3.Name a few examples of unstructured file types (HINT: the 90s tracksuit file is unstructured data): PDF, Audio, Video, Email, etc..,
4.Describe the function of Snowflake Stage Objects. (e.g. Is it a location? Is it temporary?): It's location
*/

