//Lesson 9: Lions & Tigers & Bears, Oh My!!!  📓 What Are All These Things?
use role sysadmin;
use warehouse compute_wh;
use schema MELS_SMOOTHIE_CHALLENGE_DB.TRAILS;
/*-----------------------------------------------------------------*/
/*📓  Materialized Views, External Tables,  and Iceberg Tables
Lions, and Tigers, and Bears, Oh My! -- Dorothy, Wizard of Oz.

via GIPHY

But in our case, it's:

Materialized Views, and
External Tables, and 
Iceberg Tables! 
Oh My! What are all these things?

In short, all of these objects are attempts to make your less-normalized (possibly non-loaded) data look and perform like more-normalized (possibly loaded) data. 

What are Materialized Views, External Tables and Iceberg Tables generally used for?
To provide Snowflake access to data that has not been loaded.


📓  Materialized Views
A Materialized View is like a view that is frozen in place (more or less looks and acts like a table).
The big difference is that if some part of the underlying data changes,  Snowflake recognizes the need to refresh it, automatically.
People often choose to create a materialized view if they have a view with intensive logic that they query often but that does NOT change often.  We can't use a Materialized view on any of our trails data because you can't put a materialized view directly on top of staged data. 


📓  External Tables
An External Table is a table put over the top of non-loaded data (sounds like our recent views, right?).
An External Table points at a stage folder(yep, we know how to do that!) and includes a reference to a file format (or formatting attributes) much like what we've been doing with our views for most of this workshop! Seems very straightforward and something within reach-- given what we've already learned in this workshop!

But, if we look at docs.snowflake.com the syntax for External tables looks intimidating. Let's break it down into what we can easily understand and have experience with, and the parts that are little less straightforward. 
There are other parts that are somewhat new, but that don't seem complicated. In our views we define the PATH and CAST first and then assign a name by saying AS <column name>. For the external table we just flip the order. State the column name first, then AS, then the PATH and CAST column definition. 
Also, there's a property called AUTO_REFRESH -- which seems self-explanatory!
*/

//🥋 Remember this View? Let's Look at it, then Rename It!
--We created a view called CHERRY_CREEK_TRAIL in an earlier lesson. 
--Run a SELECT * on it to remind yourself what the data looks like. 
select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL;

//We're going to create this same data structure with an External Table so let's change the name of our view to have "V_" in front of the name. That way we can create a table that starts with "T_".
alter view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL rename to MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.V_CHERRY_CREEK_TRAIL;

//🥋 Let's Create a Super-Simple, Stripped Down External Table
--Copy and paste this statement so that we can get a sense of the most stripped-down table we can create. Then we'll make it more complex. 
create or replace external table MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.T_CHERRY_CREEK_TRAIL
(my_filename varchar() as (metadata$filename::varchar())
)
location = @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET
auto_refresh = true
file_format = (type = parquet)
;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.T_CHERRY_CREEK_TRAIL;

/*
🥋 Now Let's Modify Our V_CHERRY_CREEK_TRAIL Code to Create the New Table
V_CHERRY_CREEK_TRAIL has a lot of the code we need for T_CHERRY_CREEK_TRAIL, so lets grab a copy of our view definition and use it for cutting and pasting.
We'll run the GET_DDL() function to get a copy of our view code.
select get_ddl('view','mels_smoothie_challenge_db.trails.v_cherry_creek_trail');
After you run the GET_DDL(), copy and paste the code up into the worksheet.

Notice that the use of the stage and file format are very similar to the original. The column definitions require a bit of transposition on column name, and the table definition is a little more strict in requiring you to define the data types with CAST syntax (:: is CASTING). 
After you create your new EXTERNAL table, run a SELECT * to see how similar the results are to the original view. 

🥋 Okay, Here's the Full External Table Code
You should try to build it yourself, but it's kind of intense, so here's a copy/paste version, in case you need it.

Note that in this version of the code, we have fully qualified the table name, the stage name, and the file format name by adding the database and schema. 
*/
select get_ddl('view','MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.V_CHERRY_CREEK_TRAIL');

create or replace external table MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.T_CHERRY_CREEK_TRAIL
(point_id number as ($1:sequence_1::number)
,trail_name varchar as ($1:trail_name::varchar)
,lng number(11,8) as ($1:latitude::number(11,8))
,lat number(11,8) as ($1:longitude::number(11,8))
,coord_pair varchar as (lng::varchar||' '||lat::varchar)
)
location = @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET
auto_refresh = true
file_format = MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET
;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.T_CHERRY_CREEK_TRAIL;

/*
📓 Remember those Materialized Views?
Remember a few pages ago when we told you:

We can't use a Materialized View on any of our trails data because you can't put a materialized view on top of staged data. 
Well, we left out an important detail. You CAN put a Materialized View over an External Table, even if that External Table is based on a Stage!!

In other words, you CAN put a Materialized View over staged data, as long as you put an External Table in between them, first!

🎯 Create a Materialized View on Top of the External Table
Actually, make it a Secure Materialized View and name it SMV_CHERRY_CREEK_TRAIL. You can write the code yourself or you can use the code templates available from the CREATE menu. 
Also:

Make sure it is in the TRAILS schema. 
Make sure it is owned by the SYSADMIN role. 
Make sure your view works when you run a select star. 
*/
create or replace secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL
COMMENT = 'Badge 4: Data Lake Workshop'
as
select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.T_CHERRY_CREEK_TRAIL
;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;

/*📓  Iceberg Tables
Iceberg tables are coming to Snowflake!! When? We don't want to say for sure, because it might change. You can't get hands on experience with Iceberg tables in this workshop (yet) but you won't want a Snowflake Data Lake Badge that didn't hype you up about Iceberg Tables and their enormous potential.

You'll at least want to know what they are, and what they will make possible in Snowflake!! 

Iceberg is an open-source table type, which means a private company does not own the technology. Iceberg Table technology is not proprietary. 
Iceberg Tables are a layer of functionality you can lay on top of parquet files (just like the Cherry Creek Trails file we've been using) that will make files behave more like loaded data. In this way, it's like a file format, but also MUCH more. 
Iceberg Table data will be editable via Snowflake! Read that again. Not just the tables are editable (like the table name), but the data they make available (like the data values in columns and rows). So, you will be able to create an Iceberg Table in Snowflake, on top of a set of parquet files that have NOT BEEN LOADED into Snowflake, and then run INSERT and UPDATE statements on the data using SQL 🤯. 
Iceberg Tables will make Snowflake's Data Lake options incredibly powerful!!

THIS CHANGES EVERYTHING
People sometimes think of Snowflake as a solution for structured, normalized data (which they often call a Data Warehouse). For a while, people said Data Lakes were the only path forward. Lately, many people say the best solution is a Data Lakehouse (they're just mushing the two terms together and saying you need both).

Snowflake can be all of those things and Iceberg tables will be an amazing addition. 
Watch the video below if you want to see a bit more about what's coming soon!

Optional Article on Iceberg Tables
Below, we've linked to an article about Iceberg tables in Snowflake. It gives more history on Iceberg tables and more technical details. It may be a bit advanced so don't worry if you can't process all of what is being said. 

https://www.snowflake.com/blog/5-reasons-apache-iceberg/

You may notice a nuance here.  The nuance is this - first Snowflake announced they would have an Iceberg FORMAT as a type of Snowflake External table. That use of Iceberg would not allow updates and inserts.
Later, Snowflake announced Iceberg tables as a new kind of object, not a type of External Table, but it's own thing. The whole-new-thing Iceberg tables are the ones that will enable updates and inserts. 
*/




