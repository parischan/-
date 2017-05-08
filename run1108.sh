#!/bin/sh
bin_path=/app/ELOG_DDT_CORE_R_01/bin
conf_path=/app/ELOG_DDT_CORE_R_01/conf
script_path=/app/ELOG_DDT_CORE_R_01/script
week=`date|awk '{print $1}'`
month=`date|awk '{print $3}'`
cd $bin_path
source $conf_path/conf.properties

mysql -uddtmain -pxxx  -h10.116.134.144   -P3306   -e "source $script_path/parischan008.sql"
#Rscript cust_ts_model1.r;



date_time=`date  +%Y-%m-%d`
today=`date -d"$date_time" +%Y-%m-%d`
cur_date=`date -d"$today 1 day ago" +%Y-%m-%d`
before_date=`date -d"$today 2 day ago" +%Y-%m-%d`
week_ago=`date -d"$today 7 day ago" +%Y-%m-%d`
last_year_month=`date -d"$today 1 year ago" +%Y-%m`
cur_year=`date -d"$today 1 day ago" +%Y`
cur_month=`date -d"$today 1 day ago" +%Y-%m`
month_begin=`date -d"$today 1 day ago" +%Y-%m-01`
cur_day=`date -d "$today 1 day ago" +%d`i
mkdir ${curr_dir}/LOG
log=${curr_dir}/LOG
log_date=`date  +%Y-%m-%d`
dayofweek=`date -d"$today" +%w`
dayofmonth=`date -d"$today" +%d`


#################################MONTH############################

if [ ${dayofmonth} -eq '01' ]; then
	Rscript  $bin_path/busi_pred_month0815.r ${cur_date} >>${log}/cust_pred_month_${log_date}.log;
	Rscript  $bin_path/busi_pred_month0815ct3.r ${cur_date} >>${log}/cust_pred_month_ct3_${log_date}.log;
  echo -e "Update ODS_DTE for month result finished:`date +"%Y%m%d% %H%M%S"` \n" 

else
mysql -uddtdata -pxxx  -h10.116.134.143  -P3306   -e "
use ddtdata;
set character_set_results=utf8;
set character_set_client=utf8;
set character_set_connection=utf8;
update t_ddt_rpt_waybill_predict_b set ODS_DTE='${cur_date}' where DATA_RANGE='未来1月' and ODS_DTE='${before_date}';
update t_ddt_rpt_waybill_predict_city_b set ODS_DTE='${cur_date}' where DATA_RANGE='未来1月' and ODS_DTE='${before_date}';
update t_ddt_rpt_waybill_predict_area_b set ODS_DTE='${cur_date}' where DATA_RANGE='未来1月' and ODS_DTE='${before_date}';
update t_ddt_rpt_waybill_predict_sku_b set ODS_DTE='${cur_date}' where DATA_RANGE='未来1月' and ODS_DTE='${before_date}';

"
echo -e "Update ODS_DTE for month result finished:`date +"%Y%m%d% %H%M%S"`  \n" 

fi


#######################################DAY##############################

Rscript  $bin_path/busi_pred_day0815.r 
Rscript  $bin_path/busi_pred_day0815ct3.r 
echo -e "cust_pred_day finished:`date +"%Y%m%d% %H%M%S"` \n"




#######################################WEEK###########################
if [ ${dayofweek} -eq '1' ]; then
	Rscript  $bin_path/busi_pred_week0815.r 
	Rscript  $bin_path/busi_pred_week0815ct3.r 
  echo -e "cust_pred_week finished:`date +"%Y%m%d% %H%M%S"` \n" 
else
mysql -uddtdata -pxxx  -h10.116.134.143  -P3306   -e "
USE ddtdata;
set character_set_results=utf8;
set character_set_client=utf8;
set character_set_connection=utf8;
update t_ddt_rpt_waybill_predict_b set ODS_DTE='${cur_date}' where DATA_RANGE='未来4周' and ODS_DTE='${before_date}';
update t_ddt_rpt_waybill_predict_city_b set ODS_DTE='${cur_date}' where DATA_RANGE='未来4周' and ODS_DTE='${before_date}';
update t_ddt_rpt_waybill_predict_area_b set ODS_DTE='${cur_date}' where DATA_RANGE='未来4周' and ODS_DTE='${before_date}';
update t_ddt_rpt_waybill_predict_sku_b set ODS_DTE='${cur_date}' where DATA_RANGE='未来4周' and ODS_DTE='${before_date}';
"
echo -e "Update ODS_DTE for week result finished:`date +"%Y%m%d% %H%M%S"` \n"

done 





mysql -uddtyc -p'xxx'  -h10.116.134.144  -P3306   -e "source $script_path/fencang.sql"
mysqldump -uddtyc -p'xxx'  -h10.116.134.144 -P3306 ddtyc t_ddt_fc_fencang_result_b>/app/ELOG_DDT_CORE_R_01/script/t_ddt_
fc_fencang_result_b.sql
mysql -uddtdata -pxxx  -h10.116.134.143  -P3306 -Dddtdata  -e "source $script_path/t_ddt_fc_fencang_result_b.sql"





 