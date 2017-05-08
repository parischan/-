------------------------------------------------------------------------------
----------------------------------by:cbx
---------------------------------time:2016-12-19
---------------------------------for :顺丰优选
--------------------------------------------------------------------------------

use o_elog_dvp_core;
set mapred.queue.name=dvp;
set mapred.job.queue.name=dvp;
set hive.cli.print.header=true;
set hive.exec.reducers.max=100;
set hive.exec.compress.output=false;
set hive.exec.compress.intermediate=true;
set mapred.max.split.size=1000000000;
set mapred.min.split.size.per.node=1000000000;
set mapred.min.split.size.per.rack=1000000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.groupby.skewindata=false;
set hive.auto.convert.join=true;



------------------------------取上三周数据
CREATE  TABLE if not exists tmp01_sfbest_pre_week(
product_id string, 
wh_id string, 
city string,
sell_time bigint, 
sell_price double, 
sell_num double, 
rn int)
ROW FORMAT SERDE 
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';



insert overwrite table tmp01_sfbest_pre_week 
SELECT 
t.product_id ,
t.wh_id,
t.city,
t.sell_time,
avg(t.sell_price) sell_price,
sum(t.sell_num) sell_num,
row_number() over(PARTITION BY t.product_id,t.wh_id,t.city ORDER BY t.sell_time DESC) rn
from
(
select 
detail_id,
product_id,
wh_id,
city,
product_price,
sell_price,
sell_num, 
case when weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))<10 then
concat(year(from_unixtime(bigint(sell_time),'yyyy-MM-dd')),0,weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd')))
when weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))>=10 then  
concat(year(from_unixtime(bigint(sell_time),'yyyy-MM-dd')),weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))) 
end as sell_time
FROM o_elog_dvp_core.gshop_product_sell_detail 
where from_unixtime(bigint(sell_time),'yyyy-MM-dd')<'2016-12-20'
and from_unixtime(bigint(sell_time),'yyyy-MM-dd')>='2014-05-27'
and out_in=0
group by detail_id,
product_id,
wh_id,
city,
product_price,
sell_price,
sell_num,
case when weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))<10 then
concat(year(from_unixtime(bigint(sell_time),'yyyy-MM-dd')),0,weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd')))
when weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))>=10 then  
concat(year(from_unixtime(bigint(sell_time),'yyyy-MM-dd')),weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))) 
end
) t 
group by t.product_id ,
t.wh_id,
t.city,
t.sell_time
;



CREATE  TABLE if not  exists  tmp02_sfbest_pre_week(
product_id string, 
wh_id string, 
city string,
sell_price_last1 bigint, 
sell_price_last2 bigint, 
sell_price_last3 bigint, 
sell_num_last1 double, 
sell_num_last2 double, 
sell_num_last3 double,
sell_money_last1 double, 
sell_money_last2 double,
sell_money_last3 double)
ROW FORMAT SERDE 
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';




insert overwrite table tmp02_sfbest_pre_week
select 
product_id,
wh_id,
city,
nvl(max(case when b.rn=1 then b.sell_price end),0) sell_price_last1,
nvl(max(case when b.rn=2 then b.sell_price end),0) sell_price_last2,
nvl(max(case when b.rn=3 then b.sell_price end),0) sell_price_last3,
nvl(max(case when b.rn=1 then b.sell_num end),0) sell_num_last1,
nvl(max(case when b.rn=2 then b.sell_num end),0) sell_num_last2,
nvl(max(case when b.rn=3 then b.sell_num end),0) sell_num_last3,
nvl(max(case when b.rn=1 then b.sell_num*b.sell_price end),0) sell_money_last1,
nvl(max(case when b.rn=2 then b.sell_num*b.sell_price end),0) sell_money_last2,
nvl(max(case when b.rn=3 then b.sell_num*b.sell_price end),0) sell_money_last3
from tmp01_sfbest_pre_week b
where b.rn<=3 
group by product_id,wh_id,city; 



--------------------------取近三个月数据
算的太蛋疼，直接拿了国伟的数据 tmp03_sfbest_pre
-- drop TABLE if exists tmp06_sfbest_pre_month;
-- CREATE TABLE if not exists tmp06_sfbest_pre_month(
-- product_id string, 
-- wh_id string, 
-- city string,
-- sell_time string,
-- sell_sum bigint, 
-- sell_price bigint,
-- sell_amount bigint
-- )
-- ROW FORMAT SERDE 
-- 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
-- STORED AS INPUTFORMAT 
-- 'org.apache.hadoop.mapred.TextInputFormat' 
-- OUTPUTFORMAT 
-- 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';



-- insert overwrite table tmp06_sfbest_pre_month;
-- select 
-- product_id,wh_id,city,from_unixtime(bigint(sell_time),'yyyy-MM-dd') sell_time,
-- bigint(sum(sell_num)) sell_sum,
-- avg(sell_price) sell_price,
-- sum(sell_num)*avg(sell_price) sell_amount
-- from o_elog_dvp_core.gshop_product_sell_detail  
-- where from_unixtime(bigint(sell_time),'yyyy-MM-dd')>='${running_date_90before}' 
-- and from_unixtime(bigint(sell_time),'yyyy-MM-dd')<'${running_date}'  
-- and out_in=0
-- group by product_id,wh_id,city,from_unixtime(bigint(sell_time),'yyyy-MM-dd')
-- ;




-- drop TABLE if exists tmp03_sfbest_pre_month;
-- CREATE TABLE if not exists tmp03_sfbest_pre_month(
-- product_id string, 
-- wh_id string, 
-- city string,
-- sell_avg_sum bigint, 
-- sell_min_sum bigint, 
-- sell_max_sum bigint, 
-- sell_percentile_sum bigint,
-- sell_avg_amount bigint, 
-- sell_min_amount bigint, 
-- sell_max_amount bigint, 
-- sell_percentile_amount bigint,
-- sell_avg_price bigint, 
-- sell_min_price bigint, 
-- sell_max_price bigint, 
-- sell_percentile_price bigint
-- )
-- ROW FORMAT SERDE 
-- 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
-- STORED AS INPUTFORMAT 
-- 'org.apache.hadoop.mapred.TextInputFormat' 
-- OUTPUTFORMAT 
-- 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';




-- insert overwrite table tmp03_sfbest_pre_month
-- select t.product_id,
-- t.wh_id,
-- t.city,
-- sum(t.sell_sum) / 90 sell_avg_sum,
-- min(nvl(t.sell_sum, 0)) sell_min_sum,
-- max(nvl(t.sell_sum, 0)) sell_max_sum,
-- percentile(bigint(t.sell_sum), 0.5) sell_percentile_sum,
-- sum(t.sell_amount) / 90 sell_avg_amount,
-- min(nvl(t.sell_amount, 0)) sell_min_amount,
-- max(nvl(t.sell_amount, 0)) sell_max_amount,
-- percentile(bigint(t.sell_amount), 0.5) sell_percentile_amount,
-- sum(t.sell_price) / 90 sell_avg_price,
-- min(nvl(t.sell_price, 0)) sell_min_price,
-- max(nvl(t.sell_price, 0)) sell_max_price,
-- percentile(bigint(t.sell_price), 0.5) sell_percentile_price
-- from tmp06_sfbest_pre_month t 
-- group by t.product_id, t.wh_id, t.city;




drop TABLE if exists tmp05_sfbest_pre_week_p;
CREATE TABLE if not exists tmp05_sfbest_pre_week_p as
select a.detail_id,
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
a.cooperation_mode,                                                    
a.province,                                   
a.city,                                   
a.district,                                   
a.area_num,                                         
a.area_name,                                         
a.out_in,    
a.order_id,                              
case when weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))<10 then
concat(year(from_unixtime(bigint(sell_time),'yyyy-MM-dd')),0,weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd')))
when weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))>=10 then  
concat(year(from_unixtime(bigint(sell_time),'yyyy-MM-dd')),weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))) 
end as sell_time,
avg(a.in_price) in_price,                                             
avg(a.mwaverage_price) mwaverage_price,                                   
avg(a.product_price) product_price,                                       
avg(a.sell_price) sell_price,                             
sum(a.sell_num) sell_num,                                         
sum(a.coupon_money) coupon_money,                                   
sum(a.balance_paid) balance_paid,                                     
sum(a.card_paid) card_paid
from gshop_product_sell_detail a
where a.out_in=0 
and from_unixtime(bigint(a.sell_time),'yyyy-MM-dd')>='2014-05-27'
group by a.detail_id,
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
a.cooperation_mode,
a.province,                                   
a.city,                                   
a.district,                                   
a.area_num,                                         
a.area_name,                                         
a.out_in,        
a.order_id,                            
case when weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))<10 then
concat(year(from_unixtime(bigint(sell_time),'yyyy-MM-dd')),0,weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd')))
when weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))>=10 then  
concat(year(from_unixtime(bigint(sell_time),'yyyy-MM-dd')),weekofyear(from_unixtime(bigint(sell_time),'yyyy-MM-dd'))) 
end;




-----------事先创建三个表，减少job数，提高速度(dvp已存在)
----  create table gshop_region_01 as select * from gshop_region;
----  create table gshop_region_02 as select * from gshop_region;
----  create table gshop_region_03 as select * from gshop_region;




drop table if exists tmp04_sfbest_pre_detail;
create table if not exists tmp04_sfbest_pre_detail as
select a.detail_id,
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
a.cooperation_mode,
a.in_price,
a.mwaverage_price,
a.product_price,
a.sell_price,
a.sell_num,
a.coupon_money,
a.balance_paid,
a.card_paid,
a.province,
a.city,
a.district,
a.area_num,
a.area_name,
a.out_in,
b.sell_price_last1,
b.sell_price_last2,
b.sell_price_last3,
b.sell_num_last1,
b.sell_num_last2,
b.sell_num_last3,
b.sell_money_last1,
b.sell_money_last2,
b.sell_money_last3,
c.sell_avg_day,
c.sell_min_day,
c.sell_max_day,
c.sell_percentile_day,
d.is_book,
d.is_fragile,
d.is_cod,
d.is_return,
d.is_oos,
d.new_tag,
d.is_cross_region,
d.is_nationwide,
d.sell_type,
e.shelf_life,
e.shelve_date,
e.storage_conditions,
e.storage,
e.status,
e.sale_unit,
e.specification,
e.max_unsalable,
e.advent_shelves,
e.pro_warning,
e.business_model,
e.return_policy,
h.order_type,
h.order_source,
h.order_id,
h.temperature_type,
t1.region_name province_name,
t2.region_name city_name,
t3.region_name district_name
from tmp05_sfbest_pre_week_p a
left join tmp02_sfbest_pre_week b
on a.product_id=b.product_id
and a.wh_id=b.wh_id
and a.city=b.city
left join tmp03_sfbest_pre c
on a.product_id=c.product_id
and a.wh_id=c.wh_id
left join gshop_product_ext_property d ON a.product_id=d.product_id
LEFT JOIN gshop_product e ON a.product_id=e.product_id
left join gshop_order h
on a.order_id=h.order_id
LEFT JOIN gshop_region_01 t1
ON a.province=t1.region_id
LEFT JOIN gshop_region_02 t2
ON a.city=t2.region_id
LEFT JOIN gshop_region_03 t3
ON a.district=t3.region_id
group by a.detail_id,
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
a.cooperation_mode,
a.in_price,
a.mwaverage_price,
a.product_price,
a.sell_price,
a.sell_num,
a.coupon_money,
a.balance_paid,
a.card_paid,
a.province,
a.city,
a.district,
a.area_num,
a.area_name,
a.out_in,
b.sell_price_last1,
b.sell_price_last2,
b.sell_price_last3,
b.sell_num_last1,
b.sell_num_last2,
b.sell_num_last3,
b.sell_money_last1,
b.sell_money_last2,
b.sell_money_last3,
c.sell_avg_day,
c.sell_min_day,
c.sell_max_day,
c.sell_percentile_day,
d.is_book,
d.is_fragile,
d.is_cod,
d.is_return,
d.is_oos,
d.new_tag,
d.is_cross_region,
d.is_nationwide,
d.sell_type,
e.shelf_life,
e.shelve_date,
e.storage_conditions,
e.storage,
e.status,
e.sale_unit,
e.specification,
e.max_unsalable,
e.advent_shelves,
e.pro_warning,
e.business_model,
e.return_policy,
h.order_type,
h.order_source,
h.order_id,
h.temperature_type,
t1.region_name,
t2.region_name,
t3.region_name
;


---主键去重
use o_elog_dvp_core;
drop table if exists tmp04_sfbest_pre_detaila;
create table if not exists tmp04_sfbest_pre_detaila as
select t.* 
from 
(select a.*,
row_number() over (partition by a.detail_id) num 
from tmp04_sfbest_pre_detail a
) t 
where t.num=1;

select * from tmp04_sfbest_pre_detaila limit 12;
select count(*) from tmp04_sfbest_pre_detaila;
select count(distinct detail_id) from tmp04_sfbest_pre_detaila;



