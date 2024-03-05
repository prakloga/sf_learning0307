use role sysadmin;
use warehouse compute_wh;
use schema "ZENAS_ATHLEISURE_DB"."PRODUCTS";
/*-------------------------------------------------------------------*/
//Lesson 4: Working with External Unstructured Data | 📓 External Data is Easy!

//📓 External Data is Easy, Let's Do Some More!
/*
Remember that Zena (and you) started by creating three External Stage objects to point at 3 cloud folders in an AWS S3 bucket that is owned and managed by Zena's friend, Klaus.

Zena then focused her efforts on one of those stages - the ZMD stage - that happens to contain only STRUCTURED data files. She learned to use FILE FORMATS and VIEWS to make the files very accessible without even loading her data into a Snowflake table!  

Zena has two other External Stage objects she set up. Next, she wants to use her UNI_KLAUS_CLOTHING stage, however, that stage points at a cloud folder that contains images.

Images are considered UNSTRUCTURED data, so she's wondering if accessing images without loading them will be just as easy as the flat files. 

Zena's not sure but she'll give it a try!


🎯 Run a List Command On the Clothing Stage
Remember that Zena began exploring non-loaded data by running a LIST command on the ZMD stage. Run a LIST command on the CLOTHING Stage you created. What do you see? 
*/
list @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING;

//📓 Let's Query the Unstructured External Data!
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING; --Invalid UTF8 detected in string '0x89PNG'

//🥋 Try to Query an Unstructured Data File
select $1 from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING/90s_tracksuit.png; --Invalid UTF8 detected in string '0x89PNG'

//🥋 Query with 2 Built-In Meta-Data Columns
select 
 metadata$filename
,metadata$file_row_number
from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING/90s_tracksuit.png;


//🎯 Write a Query That Returns Something More Like a List Command
/*Can you write a query that would GROUP BY the file name and look something like the results below?
Use either the MAX function or a COUNT to get an idea of the comparative file size for all the files in the stage.
*/
select 
 metadata$filename
,count(metadata$file_row_number) as number_of_rows
from @ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING
group by metadata$filename
;

//📓 File Formats for Unstructured Data? Nope. 
/*For images, we'll have to keep looking and find a better way.We'll search the online Docs for "Unstructured Data" and notice something called a "Directory Table." 
Directory Tables might work! So let's explore them further! A few important tips about Directory Tables. 

They are attached to a Stage (internal or external).  
You have to enable them. 
You have to refresh them. 
*/
//🥋 Enabling, Refreshing and Querying Directory Tables 
--Directory Tables
select * from directory(@ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING);

-- Oh Yeah! We have to turn them on, first
alter stage ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING
set directory = (enable = true);

--Now?
select * from directory(@ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING);

--Oh Yeah! Then we have to refresh the directory table!
alter stage ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING refresh;

--Now?
select * from directory(@ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING);

//📓 What About Functions for Directory Tables?
//🥋 Start By Checking Whether Functions will Work on Directory Tables 
--testing UPPER and REPLACE functions on directory table

select
 upper(RELATIVE_PATH) as uppercase_filename
,replace(uppercase_filename, '/') as no_slash_filename
,replace(no_slash_filename, '_') as no_underscores_filename
,replace(no_underscores_filename, '.PNG') as just_words_filename
from directory(@ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING)
;

//🎯 Nest 4 Functions into 1 Statement
--We did the first one for you as an example.  
select replace(replace(replace(upper(RELATIVE_PATH),'/'),'_'),'.PNG') as product_name
from directory(@ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING)
;

//🥋 Create an Internal Table in the Zena Database
--create an internal table for some sweat suit info
create or replace TABLE ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS (
	COLOR_OR_STYLE VARCHAR(25),
	DIRECT_URL VARCHAR(200),
	PRICE NUMBER(5,2)
);

--fill the new table with some data
insert into  ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS 
          (COLOR_OR_STYLE, DIRECT_URL, PRICE)
values
('90s', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/90s_tracksuit.png',500)
,('Burgundy', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/forest_green_sweatsuit.png',65)
,('Navy Blue', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/navy_blue_sweatsuit.png',65)
,('Orange', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/orange_sweatsuit.png',65)
,('Pink', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/pink_sweatsuit.png',65)
,('Purple', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/purple_sweatsuit.png',65)
,('Red', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/red_sweatsuit.png',65)
,('Royal Blue',	'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/royal_blue_sweatsuit.png',65)
,('Yellow', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/yellow_sweatsuit.png',65);

select * from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS;
select DIRECT_URL, split_part(split_part(DIRECT_URL,'//',2),'/',3) from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS;

//🎯 Can You Join These?
--This challenge lab does not include step-by-step details. Can you join the directory table and the new sweatsuits table?
select
 s.COLOR_OR_STYLE
,s.DIRECT_URL
,s.PRICE
,d.size as image_size
,d.last_modified as image_last_modified
from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS as s
join directory(@ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING) as d
on d.relative_path = substr(s.direct_url,54,50)
;

//📓  Using Functions in the ON clause of the JOIN
//📓  Adding a Cross Join
--Speaking of in-elegant solutions, Zena needs to create fake sweat suit listings for every color in the sweatsuits table, and every size in the sweatsuit_sizes view we created earlier.

//🥋 Add the CROSS JOIN 
-- 3 way join - internal table, directory table, and view based on external data

//🎯 Convert Your Select Statement to a View
--Lay a view on top of the select above and call it catalog. 
--Make sure the view is in Zena's database, in her Products schema, and is owned by the SYSADMIN role. 
create or replace view ZENAS_ATHLEISURE_DB.PRODUCTS.catalog
COMMENT = 'Badge 4: Data Lake Workshop'
as
select
 s.COLOR_OR_STYLE
,s.DIRECT_URL
,s.PRICE
,d.size as image_size
,d.last_modified as image_last_modified
,ss.sizes_available
from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS as s
join directory(@ZENAS_ATHLEISURE_DB.PRODUCTS.UNI_KLAUS_CLOTHING) as d
on d.relative_path = substr(s.direct_url,54,50)
cross join ZENAS_ATHLEISURE_DB.PRODUCTS.sweatsuit_sizes as ss
;

select * from ZENAS_ATHLEISURE_DB.PRODUCTS.catalog;

//🥋 Add the Upsell Table and Populate It
-- Add a table to map the sweat suits to the sweat band sets
create or replace table ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(SWEATSUIT_COLOR_OR_STYLE varchar(25)
,UPSELL_PRODUCT_CODE varchar(10)
)
;

--populate the upsell table
insert into ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE
,UPSELL_PRODUCT_CODE 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

//🥋 Zena's View for the Athleisure Web Catalog Prototype
-- Zena needs a single view she can query for her website prototype
create or replace view ZENAS_ATHLEISURE_DB.PRODUCTS.catalog_for_website as 
select color_or_style
,price
,direct_url
,size_list
,coalesce('BONUS: ' ||  headband_description || ' & ' || wristband_description, 'Consider White, Black or Grey Sweat Accessories')  as upsell_product_desc
from (select
 COLOR_OR_STYLE
,DIRECT_URL
,PRICE
,image_size
,image_last_modified
,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
from ZENAS_ATHLEISURE_DB.PRODUCTS.catalog
group by COLOR_OR_STYLE,DIRECT_URL,PRICE,image_size,image_last_modified
    )as c
left join ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING as u
on u.SWEATSUIT_COLOR_OR_STYLE = c.COLOR_OR_STYLE
--
left join ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_COORDINATION as sc
on sc.product_code = u.UPSELL_PRODUCT_CODE
--
left join ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_PRODUCT_LINE as spl
on spl.product_code = sc.product_code
--
where price < 200 -- high priced items like vintage sweatsuits aren't a good fit for this website
and image_size < 1000000 -- large images need to be processed to a smaller size
;


select * from ZENAS_ATHLEISURE_DB.PRODUCTS.catalog_for_website;
























