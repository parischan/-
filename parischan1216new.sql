USE ddtmain;
set character_set_results=utf8;
set character_set_client=utf8;
set character_set_connection=utf8;


drop table IF EXISTS temp_targetsku0803;
CREATE table temp_targetsku0803 as 
SELECT a.detail_id,
a.product_id,
a.wh_area,
a.product_name,
a.product_number,
a.category_one_code,
a.category_one_name,
a.category_two_code,
a.category_two_name,
a.category_three_code,
a.category_three_name,
a.brand_id,
a.brand_name,
a.wh_id,
a.wh_name,
a.sell_time,
a.sell_price,
a.sell_num,
a.province,
a.city,
a.district,
a.out_in,
a.order_type,
a.order_source,
a.order_id,
a.province_name,
a.city_name,
a.district_name
from rpt_yy_sfbest_pre a 
join productid1130 b 
on a.product_number=b.productid 
where a.product_name not like '%赠品%'
and order_source in (0,2,3,5,6,7,12)
group by a.detail_id;



alter table temp_targetsku0803 add index targetsku0803_id(detail_id);



update temp_targetsku0803 t set t.sell_time = '0' where t.sell_time = ' ';
ALTER TABLE temp_targetsku0803 MODIFY COLUMN sell_time VARCHAR(25);  
update temp_targetsku0803 t set t.sell_time = FROM_UNIXTIME(sell_time,'%Y-%m-%d');



drop table IF EXISTS temp_targetsku_week;
create table temp_targetsku_week as
SELECT DATE_FORMAT(ADDDATE(sell_time,INTERVAL -1 DAY),'%X%V') WEEKNUM,
product_number SKU,
city TO_CITY_CODE,
sum(sell_num) ORDER_NUM
from temp_targetsku0803 
where city <> 0
and sell_time>='2016-01-01'
GROUP BY 1,2,3
ORDER BY 2,1;



alter table temp_targetsku_week add index targetsku_week_id(WEEKNUM,SKU,TO_CITY_CODE);



drop table IF EXISTS temp_targetsku_month;
create table temp_targetsku_month as
select substr(sell_time,1,7) MONTHNUM,
product_number SKU,
city TO_CITY_CODE,
sum(sell_num) ORDER_NUM
from temp_targetsku0803
where city <> 0
group by 1,2,3 
ORDER BY 2,1;



alter table temp_targetsku_month add index targetsku_month_id(MONTHNUM,SKU,TO_CITY_CODE);



drop table IF EXISTS temp_targetct3_month;
create table temp_targetct3_month as
select substr(sell_time,1,7) MONTHNUM,
category_two_name SKU,
city TO_CITY_CODE,
sum(sell_num) ORDER_NUM
from temp_targetsku0803
where city <> 0
group by 1,2,3 
ORDER BY 2,1;



alter table temp_targetct3_month add index targetct3_month_id(MONTHNUM,SKU,TO_CITY_CODE);


drop table IF EXISTS temp_targetct3_week;
create table temp_targetct3_week as
SELECT DATE_FORMAT(ADDDATE(sell_time,INTERVAL -1 DAY),'%X%V') WEEKNUM,
category_two_name SKU,
city TO_CITY_CODE,
sum(sell_num) ORDER_NUM
from temp_targetsku0803 
where city <> 0
and sell_time>='2016-01-01'
GROUP BY 1,2,3
ORDER BY 2,1;



alter table temp_targetct3_week add index targetct3_week_id(WEEKNUM,SKU,TO_CITY_CODE);



drop table IF EXISTS temp_address;
create table temp_address as
SELECT province,province_name,city,city_name
from temp_targetsku0803
where city<>0
and province_name not like '%市%'
GROUP BY 4;



alter table temp_address add index address_id(city_name);



drop table IF EXISTS temp_sku_ct3;
CREATE table temp_sku_ct3 as
SELECT product_id,product_number,product_name,
category_one_code,category_one_name,
category_two_code,category_two_name
from temp_targetsku0803
GROUP BY 2;



alter table temp_sku_ct3 add index sku_ct3_id(product_number);



drop table temp_targetct3_week_all;
create TABLE temp_targetct3_week_all as 
select a.WEEKNUM,
a.SKU,
a.TO_CITY_CODE,
a.ORDER_NUM
from 
(select WEEKNUM,
'全部品类' as SKU,
TO_CITY_CODE,
SUM(ORDER_NUM) ORDER_NUM
from temp_targetct3_week
GROUP BY 1,3
) a
;


insert into temp_targetct3_week
SELECT * from temp_targetct3_week_all
;


drop TABLE temp_targetct3_month_all;
create TABLE temp_targetct3_month_all as 
select a.MONTHNUM,
a.SKU,
a.TO_CITY_CODE,
a.ORDER_NUM
from 
(select MONTHNUM,
'全部品类' as SKU,
TO_CITY_CODE,
SUM(ORDER_NUM) ORDER_NUM
from temp_targetct3_month
GROUP BY 1,3
) a
;


insert into temp_targetct3_month
SELECT * from temp_targetct3_month_all
;



