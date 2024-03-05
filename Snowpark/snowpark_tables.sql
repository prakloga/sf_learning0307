//Set worksheet Context
use role sysadmin;
use warehouse snowpark_wh;
use database snowpark_db;
use schema public;
/*------------------------------------------------------------------------------------*/
create or replace table stock_price_history (company TEXT, price_date DATE, price INT, quantity int);
--
insert into stock_price_history values
    ('ABCD', '2020-10-01', 50, 1),
    ('XYZ' , '2020-10-01', 89, 2),
    ('ABCD', '2020-10-02', 36, 3),
    ('XYZ' , '2020-10-02', 24, 4),
    ('ABCD', '2020-10-03', 39, 5),
    ('XYZ' , '2020-10-03', 37, 6),
    ('ABCD', '2020-10-04', 42, 7),
    ('XYZ' , '2020-10-04', 63, 8),
    ('ABCD', '2020-10-05', 30, 9),
    ('XYZ' , '2020-10-05', 65, 10),
    ('ABCD', '2020-10-06', 47, 11),
    ('XYZ' , '2020-10-06', 56, 12),
    ('ABCD', '2020-10-07', 71, 14),
    ('XYZ' , '2020-10-07', 50, 14),
    ('ABCD', '2020-10-08', 80, 15),
    ('XYZ' , '2020-10-08', 54, 16),
    ('ABCD', '2020-10-09', 75, 17),
    ('XYZ' , '2020-10-09', 30, 18),
    ('ABCD', '2020-10-10', 63, 19),
    ('XYZ' , '2020-10-10', 32, 20);
--
select * from stock_price_history;

select * from FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY limit 100;