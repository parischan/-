-- 商品销售明细
-- 下单时间貌似是秒级，需要转换；
SELECT MAX(sell_time) from gshop_sell_product_detail
-- 2013-04-22 10:28:24
;
--
SELECT MAX(sell_time) from gshop_product_sell_detail
;

SELECT DISTINCT category_one_name from gshop_sell_product_detail
;
-- 少一个商品一级分类
SELECT DISTINCT category_one_name from gshop_product_sell_detail
;




-- gshop_browse_recommendation 	浏览商品推荐
-- gshop_buy_recommendation 	购买商品推荐
-- gshop_view_buy_recommendation	浏览购买商品推荐 
-- gshop_category_brand_relation 	分类关联品牌排序表

-- gshop_disperse_order_product	散单订单商品表    
-- gshop_favourable_activity	优惠活动表            ！                         
-- gshop_favourable_range	优惠活动关联范围表          ！                              
-- gshop_lc_order_product	大客户订单商品表
-- gshop_month_sell_modulus	月参考销售趋势系数表 
 

select * from gshop_browse_recommendation  limit 10;
select * from gshop_buy_recommendation  limit 10;
select * from gshop_category_brand_relation  limit 10;
select * from gshop_disperse_order_product limit 10;
select * from gshop_favourable_activity limit 10;
select * from gshop_favourable_range limit 10;
select * from gshop_lc_order_product limit 10;
select * from gshop_month_sell_modulus limit 10;

-- gshop_order_active	订单优惠活动表                                        
-- gshop_order_product	订单商品表          ！ 跑不出                                   
-- 	
-- gshop_product	商品表                ！                        
-- gshop_product_activity_flag 	商品活动标志表		！
-- gshop_product_ext_category 	商品扩展分类表
-- gshop_product_ext_property	商品扩展属性表           ！                              
-- 	
-- gshop_product_sell_detail	商品销售明细              ！                           
-- gshop_score_product 	商品评分表
-- gshop_sell_product_detail 	商品销售明细			  ！
-- gshop_view_buy_recommendation	浏览购买商品推荐 

-- #####
select * from gshop_order_active limit 10;
select * from gshop_order_product limit 10;

select * from gshop_product limit 10;
select * from gshop_product_activity_flag  limit 10;
select * from gshop_product_ext_category  limit 10;
select * from gshop_product_ext_property limit 10;

select * from gshop_product_sell_detail limit 10;
select * from gshop_score_product  limit 10;
select * from gshop_sell_product_detail  limit 10;
select * from gshop_view_buy_recommendation limit 10;
