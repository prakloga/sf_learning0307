use role sysadmin;
--use warehouse compute_wh;
use warehouse intl_wh;
use schema intl_db.public;
/*---------------------------------------------------------*/
//Lesson 5: Understanding Snowflake Costs 

//🥋 Navigate to the Pricing Guide
--https://www.snowflake.com/pricing/pricing-guide/

//📓 Alinta Explains Storage Costs to ACME
/*
If Lottie decides to ONLY use Snowflake for picking up data shared by WDE, her storage costs will be zero. This is because Osiris' company pays for storage. 
Later, if Lottie decides to move some of her operational data to Snowflake, she'll need to revisit the estimate. 
*/

//🥋 Compare Storage Costs for Several Cloud/Region Choices
/*
To look up storage costs, we'll visit a pricing pages we've already used a few times already. 

This one: https://www.snowflake.com/pricing/
*/

//📓 Lottie Agrees that Storage Will Be Cheap
--Even if Lottie begins to store a lot more of her data in Snowflake, she does not think it will exceed a terabyte for quite a long time. She's very happy to realize that monthly storage on Snowflake is likely to be very cheap for her. 

//📓 Cost of a Credit, sure. But what do we do with that?
--Once we know the cost of a credit for a given cloud/region/edition we can use that to calculate monthly compute costs. The formula requires 3 pieces of information. 
--Credit Cost X Credit Per hour X Hours

//📓 Credits Per Hour
--Credits used per hour depend on the size of the warehouse. For that we can use this page again: https://www.snowflake.com/pricing/pricing-guide/
--Twenty minutes a month is just 1/3 of one hour.  $3 x 1/3 is 99 cents. ACME is likely to pay just 99 cents a month for compute!

//📓 Two Down, Two to Go
/*
Remember there are 4 categories of cost. So far we've covered:

Storage $0 per month. 
Compute $.99 per month. 
*/

//📓 Cloud Services Costs = $0
/*
Scroll down to the Cloud Services section of the https://www.snowflake.com/pricing/pricing-guide/ page. 

Based on what is explained in this section, ACME is likely to get their Cloud Services for free. 
After all, they have very low storage and compute costs and they are not using sophisticated optimization techniques that keep compute costs low enough to put them outside of what is considered "typical utilization." 
*/

//📓 Serverless Costs = $0
/*Looking at the list of Serverless Features, it may jump out at you that we saw Snowflake running Replication services. Keep in mind that replication will happen on the PROVIDING account (WDE) and not on the CONSUMING account (ACME). Again, as long as ACME only extracts the shared data, Snowflake can be expected to cost her well below $5/mo.
*/

//📓 Costs So Low, they Seem Too Good to Be True?
/*
Now we've covered all four cost categories associated with a Snowflake Account. 

Storage $0 per month. 
Compute $.99 per month. 
Cloud Services $0 per month.
Serverless $0 per month.
Lottie is certain this sounds too good to be true. She can't shake the feeling that the math is not mathing. She needs some reassurance. 
*/

//🥋 Set Up A Resource Monitor to Safeguard Spending on Your Snowflake Trial
/*
NOTE: Just because the the resource monitor notifies and suspends at 95% usage, doesn't mean you won't use 100% of the quota. There is some reporting lag time that can come into play so that your resource monitor is not able to shut things down at exactly 95%. If you need to be more conservative in keeping costs below a certain amount, you might use settings like 70/80/90 or even 65/75/85. 
*/

//🎯 Resource Monitor Challenge Lab!
/*
Set up a similar resource monitor in the ACME account.
Allow 5 credits of usage per WEEK at the Account level.
Name your Resource Monitor Weekly_5.
Use the 95/85/75 action settings we used in the other monitor.
*/

//🎯 Activate Email Notifications
--Go into the USER profiles for both your Trial Account and your ACME Account and check the box that makes it possible for you to receive emails when your resource monitors hit 75%. 


