#!/bin/bash
source /etc/profile
#***************************************
#模块：商业灯塔周特征脚本
#作者：陈碧璇
#功能：生成周特征数据
#创建时间：2016-12-29
#修改时间：
#***************************************

#路径参数定义
log=/LOG/SHELL/O_ELOG_DVP_CORE/ddt 

#时间参数定义
beg_s=`date -d " 2014-05-01 " +%s`
end_s=`date -d " 2016-10-12 " +%s`
date=`date -d "1970-01-01 UTC $beg_s seconds" +%Y-%m-%d`
incr_date=$1

echo -e "${incr_date}周特征处理开始:`date +"%Y-%m-%d %H:%M:%S"`\n">>${log}/a_${incr_date}.log

while [ "$beg_s" -le "$end_s" ]  
    do
      beg_s=$((beg_s+86400))
      beg_90s=$((beg_s-86400*90))
      running_date=`date -d "1970-01-01 UTC $beg_s seconds" +%Y-%m-%d`
      running_date_90before=`date -d "1970-01-01 UTC $beg_90s seconds" +%Y-%m-%d`
      echo $running_date
      echo $running_date_90before
hive -hivevar running_date=${running_date} -hivevar running_date_90before=${running_date_90before} -f test.q --verbose >>${log}/paris1229.log 2>&1

done

echo -e "${incr_date}周特征处理结束:`date +"%Y-%m-%d %H:%M:%S"`\n">>${log}/a_${incr_date}.log


