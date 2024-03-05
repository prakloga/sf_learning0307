/*
❕ Setting Up Your Trial Account
Check to see if you have a warehouse named COMPUTE_WH that is sized XS and will auto-suspend. If you don't have one, create it. 
Check to see if you have a database named UTIL_DB and if you don't have one, consider creating one that can serve as the home to your DORA GRADER function. 
Make sure the SYSADMIN role owns the COMPUTE_WH, the UTIL_DB database, and the PUBLIC schema of the UTIL_DB database. 
*/

/*🎯 Setting Some Defaults
You may want to set some DEFAULT values for your USER. 

These make using Snowflake more convenient by setting your worksheet context for you automatically.

Kishore's USERNAME in his Snowflake Account is KISHOREK (it's what he uses to login to Snowflake).

He runs the following commands:
*/
alter user PLOGANATHAN set default_role = 'SYSADMIN';
alter user PLOGANATHAN set default_warehouse = 'COMPUTE_WH';
alter user PLOGANATHAN set default_namespace = 'UTIL_DB.PUBLIC';

//🔠 Your Account Locator 
select current_account(); --LWB18040

/*
 🏁 Ready to Mark Lesson 1 Complete? 
Are all these statements true for you?
You are using a Snowflake Trial Account, not your employer's account and not the account of another learner. 
You have saved and submitted the ACCOUNT LOCATOR of your Snowflake Trial Account on the DORA is Listening page. 
You have run the DORA script that adds an API Integration to your account. 
You have run the DORA script that adds a function called GRADER to your account and you know which database and schema are home to this function.
You understand how object OWNERSHIP and CURRENT ROLE effect your ability to access and use Snowflake objects. 
You understand where WORKSHEET CONTEXT settings are shown, how they affect Snowflake, and how to change them. 
If all these statements are true for your Snowflake Trial Account, you should mark this lesson complete!
*/









