#cleaning all things
rm(list=ls())

#set working file directory 
#> getwd()
#[1] "C:/Users/01108340/Documents" 

###load data
#install.packages("RMySQL")
#install.packages("dplyr")
#install.packages("gbm")
#install.packages("data.table")
#install.packages("ggplot2")
#install.packages("lubridate")
#install.packages("DBI")

library(RMySQL)
library(dplyr)
library(gbm)
#library(ggplot2)
library(data.table)
library(lubridate)
library(DBI)
#library(caret)
library(Metrics)
library(RJDBC)

m1<-dbConnect(RMySQL::MySQL(),host="10.202.5.139",port=3306,
              dbname="xxx",
              user="xxx",
              password="xxx",
              characterset='utf-8')

drv<- RJDBC::JDBC('com.mysql.jdbc.Driver', "mysql-connector-java-5.1.6.jar")
m1 <- RJDBC::dbConnect(drv, "jdbc:mysql://10.202.5.139:3306/xxx", "xxx", "xxx")



#connect shengxian data 
con3 = dbGetQuery(m1,'select * from temp_train_data_week where category_one_code=8')



#solving Chinese problem 
Encoding(con3$product_name)<-'UTF-8'
Encoding(con3$category_one_name)<-'UTF-8'
Encoding(con3$category_two_name)<-'UTF-8'
Encoding(con3$category_three_name)<-'UTF-8'
Encoding(con3$brand_name)<-'UTF-8'
Encoding(con3$wh_name)<-'UTF-8'
Encoding(con3$storage_conditions)<-'UTF-8'
Encoding(con3$area_name)<-'UTF-8'
Encoding(con3$province_name)<-'UTF-8'
Encoding(con3$city_name)<-'UTF-8'
Encoding(con3$district_name)<-'UTF-8'
Encoding(con3$sale_unit)<-'UTF-8'



# Data exploration

summary(con3)


# a=head(con3);View(a)
# 
# str(con3)

#transform into data.frame
con31=data.frame(con3)

con31=filter(con31,card_paid>=0&shelf_life<=700&brand_id>1)



# str(con31_test$sell_time)
# con31$sell_time=as.Date(con31$sell_time)
# con31$season=time2season(con31$sell_time,out.fmt="seasons")
# con31$season=as.factor(con31$season)



# Data cleaning分开训练和测试样本
# Divice into 80:20
#smp <- sample(1:dim(con31)[1],size = ceiling(length(con31[,1]))*0.8)
#con31_train_ran <- con31[smp,]
#con31_test_ran <- con31[-smp,]

max(con31$sell_time)
min(con31$sell_time)
# con31$sell_time=as.numeric(con31$sell_time)
# mm=as.data.frame(summarize(group_by(con31$sell_time),
#                            sell_num=sum(sell_num)))
# Base on Date
con31_train <- con31[con31$sell_time>='201401'&con31$sell_time<='201553',]
con31_test <- con31[con31$sell_time>='201601',]

# factor processing 
con31_train$category_one_code=as.factor(con31_train$category_one_code)
con31_train$category_two_code=as.factor(con31_train$category_two_code)
con31_train$category_three_code=as.factor(con31_train$category_three_code)
con31_train$brand_id=as.factor(con31_train$brand_id)
con31_train$wh_id=as.factor(con31_train$wh_id)
con31_train$cooperation_mode=as.factor(con31_train$cooperation_mode)

# con31_train$is_promote=as.factor(con31_train$is_promote)
con31_train$is_book=as.factor(con31_train$is_book)
con31_train$is_fragile=as.factor(con31_train$is_fragile)
con31_train$is_cod=as.factor(con31_train$is_cod)
con31_train$is_return=as.factor(con31_train$is_return)
con31_train$is_oos=as.factor(con31_train$is_oos)
# con31_train$is_seasonal=as.factor(con31_train$is_seasonal)
# con31_train$is_multicity=as.factor(con31_train$is_multicity)


con31_train$is_cross_region = ifelse(con31_train$is_cross_region =='NULL'|con31_train$is_cross_region =='','is_cross_region为空',con31_train$is_cross_region)
con31_train$is_cross_region=as.factor(con31_train$is_cross_region)

con31_train$is_nationwide = ifelse(con31_train$is_nationwide =='NULL'|con31_train$is_nationwide =='','is_nationwide为空',con31_train$is_nationwide)
con31_train$is_nationwide=as.factor(con31_train$is_nationwide)
con31_train$sell_type = ifelse(con31_train$sell_type =='NULL'|con31_train$sell_type =='','sell_type为空',con31_train$sell_type)
con31_train$sell_type=as.factor(con31_train$sell_type)



con31_train$storage = ifelse(con31_train$storage =='NULL'|con31_train$storage =='','storage为空',con31_train$storage)
tmp = table(con31_train$storage)
l = names(tmp[order(table(con31_train$storage),decreasing = T)[1:50]])
con31_train$storage = as.factor(ifelse(con31_train$storage %in% l, con31_train$storage, '其它未知存储条件'))


con31_train$status = ifelse(con31_train$status =='NULL'|con31_train$status =='','status为空',con31_train$status)
tmp = table(con31_train$status)
l = names(tmp[order(table(con31_train$status),decreasing = T)[1:50]])
con31_train$status = as.factor(ifelse(con31_train$status %in% l, con31_train$status, '商品状态'))



con31_train$storage_conditions = ifelse(con31_train$storage_conditions =='NULL'|con31_train$storage_conditions =='','storage_conditions为空',con31_train$storage_conditions)
tmp = table(con31_train$storage_conditions)
l = names(tmp[order(table(con31_train$storage_conditions),decreasing = T)[1:50]])
con31_train$storage_conditions = as.factor(ifelse(con31_train$storage_conditions %in% l, con31_train$storage_conditions, '存储条件'))


con31_train$sale_unit = ifelse(con31_train$sale_unit =='NULL'|con31_train$sale_unit =='','sale_unit为空',con31_train$sale_unit)
tmp = table(con31_train$sale_unit)
l = names(tmp[order(table(con31_train$sale_unit),decreasing = T)[1:50]])
con31_train$sale_unit = as.factor(ifelse(con31_train$sale_unit %in% l, con31_train$sale_unit, '存储条件'))

con31_train$specification = ifelse(con31_train$specification =='NULL'|con31_train$specification =='','specification为空',con31_train$specification)
tmp = table(con31_train$specification)
l = names(tmp[order(table(con31_train$specification),decreasing = T)[1:50]])
con31_train$specification = as.factor(ifelse(con31_train$specification %in% l, con31_train$specification, '存储条件'))



con31_train$business_model = ifelse(con31_train$business_model =='NULL'|con31_train$business_model =='','business_model为空',con31_train$business_model)
tmp = table(con31_train$business_model)
l = names(tmp[order(table(con31_train$business_model),decreasing = T)[1:50]])
con31_train$business_model = as.factor(ifelse(con31_train$business_model %in% l, con31_train$business_model, '商品归属'))


con31_train$return_policy = ifelse(con31_train$return_policy =='NULL'|con31_train$return_policy =='','return_policy为空',con31_train$return_policy)
tmp = table(con31_train$return_policy)
l = names(tmp[order(table(con31_train$return_policy),decreasing = T)[1:50]])
con31_train$return_policy = as.factor(ifelse(con31_train$return_policy %in% l, con31_train$return_policy, '退换货条件'))

###

con31_train$order_type=as.factor(con31_train$order_type)
con31_train$order_source=as.factor(con31_train$order_source)
con31_train$temperature_type=as.factor(con31_train$temperature_type)

###目前是秒级
#con31_train$shelve_date = ifelse(con31_train$shelve_date =='NULL'|con31_train$shelve_date =='','shelve_date为空',con31_train$shelve_date)
#con31_train$shelve_date = as.Date(con31_train$shelve_date)


###ADD
con31_train$product_id=as.factor(con31_train$product_id)
con31_train$wh_area=as.factor(con31_train$wh_area)
con31_train$product_name=as.factor(con31_train$product_name)
con31_train$product_number=as.factor(con31_train$product_number)
con31_train$category_one_name=as.factor(con31_train$category_one_name)
con31_train$category_two_name=as.factor(con31_train$category_two_name)
con31_train$category_three_name=as.factor(con31_train$category_three_name)
con31_train$brand_name=as.factor(con31_train$brand_name)
con31_train$wh_name=as.factor(con31_train$wh_name)

# con31_train$sell_time=as.Date(con31_train$sell_time)

con31_train$province=as.factor(con31_train$province)
con31_train$city=as.factor(con31_train$city)
con31_train$district=as.factor(con31_train$district)
con31_train$area_num=as.factor(con31_train$area_num)
con31_train$area_name=as.factor(con31_train$area_name)
con31_train$out_in=as.factor(con31_train$out_in)



con31_train$order_type=as.factor(con31_train$order_type)
con31_train$order_source=as.factor(con31_train$order_source)
con31_train$order_id=as.factor(con31_train$order_id)
con31_train$temperature_type=as.factor(con31_train$temperature_type)




# numeric processing
# as.numeric并转换缺失数值型为-1
con31_train$in_price = as.numeric(con31_train$in_price)
con31_train$mwaverage_price = as.numeric(con31_train$mwaverage_price)
con31_train$product_price = as.numeric(con31_train$product_price)
con31_train$sell_price = as.numeric(con31_train$sell_price)
con31_train$coupon_money = as.numeric(con31_train$coupon_money)
con31_train$balance_paid = as.numeric(con31_train$balance_paid)
con31_train$card_paid = as.numeric(con31_train$card_paid)

con31_train$sell_price_last1 = as.numeric(con31_train$sell_price_last1)
con31_train$sell_price_last1 = ifelse(is.na(con31_train$sell_price_last1),-1,con31_train$sell_price_last1)

con31_train$sell_price_last2 = as.numeric(con31_train$sell_price_last2)
con31_train$sell_price_last2 = ifelse(is.na(con31_train$sell_price_last2),-1,con31_train$sell_price_last2)

con31_train$sell_price_last3 = as.numeric(con31_train$sell_price_last3)
con31_train$sell_price_last3 = ifelse(is.na(con31_train$sell_price_last3),-1,con31_train$sell_price_last3)

con31_train$sell_num_last1 = as.numeric(con31_train$sell_num_last1)
con31_train$sell_num_last1 = ifelse(is.na(con31_train$sell_num_last1),-1,con31_train$sell_num_last1)

con31_train$sell_num_last2 = as.numeric(con31_train$sell_num_last2)
con31_train$sell_num_last2 = ifelse(is.na(con31_train$sell_num_last2),-1,con31_train$sell_num_last2)

con31_train$sell_num_last3 = as.numeric(con31_train$sell_num_last3)
con31_train$sell_num_last3 = ifelse(is.na(con31_train$sell_num_last3),-1,con31_train$sell_num_last3)

con31_train$sell_money_last1 = as.numeric(con31_train$sell_money_last1)
con31_train$sell_money_last1 = ifelse(is.na(con31_train$sell_money_last1),-1,con31_train$sell_money_last1)

con31_train$sell_money_last2 = as.numeric(con31_train$sell_money_last2)
con31_train$sell_money_last2 = ifelse(is.na(con31_train$sell_money_last2),-1,con31_train$sell_money_last2)

con31_train$sell_money_last3 = as.numeric(con31_train$sell_money_last3)
con31_train$sell_money_last3 = ifelse(is.na(con31_train$sell_money_last3),-1,con31_train$sell_money_last3)


con31_train$sell_avg_day = as.numeric(con31_train$sell_avg_day)
con31_train$sell_avg_day = ifelse(is.na(con31_train$sell_avg_day),-1,con31_train$sell_avg_day)

con31_train$sell_min_day = as.numeric(con31_train$sell_min_day)
con31_train$sell_max_day = as.numeric(con31_train$sell_max_day)
con31_train$sell_percentile_day = as.numeric(con31_train$sell_percentile_day)

con31_train$shelf_life = as.numeric(con31_train$shelf_life)
con31_train$shelf_life = ifelse(is.na(con31_train$shelf_life),-1,con31_train$shelf_life)

con31_train$max_unsalable = as.numeric(con31_train$max_unsalable)
con31_train$max_unsalable = ifelse(is.na(con31_train$max_unsalable),-1,con31_train$max_unsalable)

con31_train$advent_shelves = as.numeric(con31_train$advent_shelves)
con31_train$advent_shelves = ifelse(is.na(con31_train$advent_shelves),-1,con31_train$advent_shelves)

con31_train$pro_warning = as.numeric(con31_train$pro_warning)
con31_train$pro_warning = ifelse(is.na(con31_train$pro_warning),-1,con31_train$pro_warning)




##########################GBM########################
reg_var=c('category_one_code',
          'category_two_code',
          'category_three_code',
          'brand_id',
          'wh_id',
          'cooperation_mode',
          
          
          'in_price',
          'mwaverage_price',
          'product_price',
          'sell_price',
          
          'coupon_money',
          'balance_paid',
          'card_paid',
          
          
          'sell_price_last1',
          'sell_price_last2',
          'sell_price_last3',
          'sell_num_last1',         
          'sell_num_last2',
          'sell_num_last3',
          'sell_money_last1',
          'sell_money_last2',
          'sell_money_last3',
          
          'sell_avg_day',
          'sell_min_day',
          'sell_max_day',
          'sell_percentile_day',
          
          'is_book',
          'is_fragile',
          'is_cod',
          'is_return',
          'is_oos',
          'is_cross_region',
          'is_nationwide',          
          'shelf_life',
          
          'storage_conditions',
          'storage',
          'status',
          
          'order_type',
          'order_source',
          'sale_unit',
          'specification',
          'temperature_type',
          'max_unsalable',
          'advent_shelves',
          'pro_warning',
          'business_model',
          'return_policy'
          
)

gc()
rm(con3)
fm_reg=as.formula(paste("sell_num~ ", paste(reg_var,collapse="+")))
con31_gbm_reg=gbm(formula=fm_reg,data=con31_train,distribution="gaussian",verbose=T,
                  n.trees=500,interaction.depth=8,shrinkage = 0.1)


#random sample between train and test
save(con31_gbm_reg, file='C:/Users/01108340/Documents/con31_gbm_reg_20170112shengxian.rdata')
# save(con31_test,  file='C:/Users/01108340/Documents/test_shengxian_data_20160901.rdata')
load('C:/Users/01108340/Documents/con31_gbm_reg_20170112shengxian.rdata')

# 用交叉检验确定最佳迭代次数
best.iter <- gbm.perf(con31_gbm_reg,plot.it = TRUE)
print(best.iter)
#[1]  112 

# train error
con31_gbm_reg$train.error[best.iter]
#[1] 1.534819

# 观察各解释变量的重要程度
summary(con31_gbm_reg,best.iter)
# var      rel.inf
# sell_price                   sell_price 18.397651431
# order_source               order_source 17.955696183
# coupon_money               coupon_money 17.008196542
# brand_id                       brand_id  8.935003558
# in_price                       in_price  6.626707013
# sell_num_last3           sell_num_last3  6.237266289
# sell_price_last3       sell_price_last3  5.417932634
# card_paid                     card_paid  5.271843034
# product_price             product_price  2.364963391
# sell_num_last2           sell_num_last2  1.665611800
# specification             specification  1.241238490
# mwaverage_price         mwaverage_price  1.236704670
# sell_price_last2       sell_price_last2  0.949989165
# order_type                   order_type  0.934244026
# sale_unit                     sale_unit  0.675357389
# category_three_code category_three_code  0.660777972
# temperature_type       temperature_type  0.625500803
# max_unsalable             max_unsalable  0.566330684
# sell_num_last1           sell_num_last1  0.561471930
# sell_price_last1       sell_price_last1  0.545840745
# sell_money_last1       sell_money_last1  0.516515516
# wh_id                             wh_id  0.413597244
# sell_avg_day               sell_avg_day  0.333272467
# sell_percentile_day sell_percentile_day  0.286828822
# sell_money_last2       sell_money_last2  0.214510832
# sell_money_last3       sell_money_last3  0.115496980
# sell_max_day               sell_max_day  0.084825541
# cooperation_mode       cooperation_mode  0.041969722
# category_two_code     category_two_code  0.038579930
# storage_conditions   storage_conditions  0.028992119
# sell_min_day               sell_min_day  0.020477678
# pro_warning                 pro_warning  0.009435190
# advent_shelves           advent_shelves  0.006664536
# shelf_life                   shelf_life  0.004440724
# status                           status  0.003066521
# is_fragile                   is_fragile  0.002998427
# category_one_code     category_one_code  0.000000000
# balance_paid               balance_paid  0.000000000
# is_book                         is_book  0.000000000
# is_cod                           is_cod  0.000000000
# is_return                     is_return  0.000000000
# is_oos                           is_oos  0.000000000
# is_cross_region         is_cross_region  0.000000000
# is_nationwide             is_nationwide  0.000000000
# storage                         storage  0.000000000
# business_model           business_model  0.000000000
# return_policy             return_policy  0.000000000

# # compactly print the first and last trees for curiosity
# print(pretty.gbm.tree(con31_gbm_reg,1))
# print(pretty.gbm.tree(con31_gbm_reg,con31_gbm_reg$n.trees))



########################################################################
#  predict
########################################################################
#在训练数据上看下对一致数据的预测效果 
con31_train$p_sell_num = predict(con31_gbm_reg,con31_train,n.trees = gbm.perf(con31_gbm_reg))
con31_train$p_sell_num = ifelse(con31_train$p_sell_num<0,0,con31_train$p_sell_num)
con31_train$p_sell_num = floor(con31_train$p_sell_num)

con31_train$error = abs(con31_train$p_sell_num - con31_train$sell_num)
summary(con31_train$error)
# Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# 0.0000   0.0000   0.0000   0.4096   1.0000 757.0000 


#在测试数据上预测每个sku各仓分配占比
con31_test$p_sell_num = predict(con31_gbm_reg,con31_test,n.trees = gbm.perf(con31_gbm_reg))
con31_test$p_sell_num = ifelse(con31_test$p_sell_num<0,0,con31_test$p_sell_num)
# con31_test$p_sell_num = floor(con31_test$p_sell_num)*2

con31_test$error = abs(con31_test$p_sell_num - con31_test$sell_num)
summary(con31_test$error)
# Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# 0.00896  0.31020  0.41110  0.53490  0.51850 58.44000  




#sell_date-total
con31_train_date = as.data.frame(summarize(group_by(con31_train,sell_time), 
                                           total_p_sell_num = round(sum(p_sell_num)),
                                           total_sell_num = round(sum(sell_num)),
                                           error=round(total_p_sell_num-total_sell_num)
));


write.table(con31_train_date,file='C:/Users/01108340/Documents/con31_train_date4.csv', 
            fileEncoding = "UTF-8",append = FALSE,
            col.names = TRUE,row.names = FALSE,quote = F, sep = ",")


con31_test_date = as.data.frame(summarize(group_by(con31_test,sell_time), 
                                          total_p_sell_num = round(sum(p_sell_num)),
                                          total_sell_num = round(sum(sell_num)),
                                          error=round(total_p_sell_num-total_sell_num)
));


write.table(con31_test_date,file='C:/Users/01108340/Documents/con31_test_date4.csv', 
            fileEncoding = "UTF-8",append = FALSE,
            col.names = TRUE,row.names = FALSE,quote = F, sep = ",")



############Error Analysis

#mean absolute error 
mae(con31_test$sell_num,con31_test$p_sell_num)
#  0.5348895
mse(con31_test$sell_num,con31_test$p_sell_num)
#  1.171303
rmse(con31_test$sell_num,con31_test$p_sell_num)
#  1.082267
