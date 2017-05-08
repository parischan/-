#!/bin/bash

source /etc/profile
source /OPT/SHELL/O_ELOG_DVP_CORE/ddt/dog_sqoop_data.cfg
#*****************************************************
#模块：周数据明细
#作者：陈碧璇
#功能：
#创建时间：2017-01-04
#修改时间：
#*****************************************************

#时间参数定义
incr_date=2017-01-04
incr_month=$2
delete_month=$3


#路径参数定义
log=/LOG/SHELL/O_ELOG_DVP_CORE/ddt

sqoop_module_name='周数据明细'
sqoop_src_columns='detail_id,product_id,wh_area,product_name,product_number,category_one_code,category_one_name,category_two_code,category_two_name,category_three_code,category_three_name,brand_id,brand_name,wh_id,wh_name,sell_time,cooperation_mode,in_price,mwaverage_price,product_price,sell_price,sell_num,coupon_money,balance_paid,card_paid,province,city,district,area_num,area_name,out_in,sell_price_last1,sell_price_last2,sell_price_last3,sell_num_last1,sell_num_last2,sell_num_last3,sell_money_last1,sell_money_last2,sell_money_last3,sell_avg_day,sell_min_day,sell_max_day,sell_percentile_day,is_book,is_fragile,is_cod,is_return,is_oos,new_tag,is_cross_region,is_nationwide,sell_type,shelf_life,shelve_date,storage_conditions,storage,status,sale_unit,specification,max_unsalable,advent_shelves,pro_warning,business_model,return_policy,order_type,order_source,order_id,temperature_type,province_name,city_name,district_name'
sqoop_src_table='tmp04_sfbest_pre_detail'
sqoop_dir_schemas='O_ELOG_DVP_CORE'
sqoop_dir_table='tmp04_sfbest_pre_detail'
log_item='sqoopto_tmp04_sfbest_pre_detail'

echo -e "同步${sqoop_module_name}数据处理_${incr_date}_开始于`date +"%Y-%m-%d %H:%M:%S"` \n">>${log}/${log_item}_etl_${incr_date}.log

sqoop eval -D mapred.queue.name=dvp -D mapred.job.queue.name=dvp \
           --connect jdbc:mysql://${MysqlHost}:${MysqlPort}/${MysqlDB}?characterEncoding=UTF-8 \
           --username ${MysqlUser} --password ${MysqlPwd} \
           -e "delete from ${sqoop_src_table}" \
           --verbose  >>${log}/${log_item}_etl_${incr_date}.log  2>&1
sqoop export  -D mapred.queue.name=dvp -D mapred.job.queue.name=dvp \
              --connect jdbc:mysql://${MysqlHost}:${MysqlPort}/${MysqlDB}?characterEncoding=UTF-8 \
              --username ${MysqlUser} \
              --password ${MysqlPwd} \
              --table ${sqoop_src_table} \
              --export-dir /user/hive/warehouse/o_elog_dvp_core.db/${sqoop_dir_table} \
              --columns ${sqoop_src_columns} \
              --fields-terminated-by '\001' \
              --input-null-string '\\N'  \
              --input-null-non-string  '\\N'  -m 1 \
              --verbose  >>${log}/${log_item}_etl_${incr_date}.log  2>&1


#推送一条日志给mysql

echo -e "同步${sqoop_module_name}数据处理_${incr_date}_结束于`date +"%Y-%m-%d %H:%M:%S"` \n">>${log}/${log_item}_etl_${incr_date}.log