demo示例
===================

## 一、kafka里的模拟源数据：
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/sourceData.png)

## 二、任务定义：
##### 1、用spark-stream从kafka消费数据，并注册成表任务
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/page_source.png)
***
<br />
##### 2、用spark-sql构建清洗表模型
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/page_model1.png)
***
<br />
##### 3、用spark-sql和cassandra构建新用户模型
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/page_model2.png)
***
<br />
##### 4、用spark-sql和redis统计，并将结果发送至kafka
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/page_day_uv.png)
***
<br />
##### 5、用spark-sql和redis统计，并将结果发送至kafka
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/page_hour_ref_uv.png)
***
<br />
##### 6、用spark-sql和redis统计，并将结果发送至kafka
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/page_online_cnt.png)
***
<br />
##### 7、用spark-sql和redis统计，并将结果发送至kafka
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/page_online_time.png)
***
<br />
##### 8、用spark-sql、cassandra和redis统计，并将结果发送至kafka
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/page_user_first_ref.png)

## 三、后台运行程序：
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/server.png)

## 三、从kafka中查询统计结果：每小时各渠道UV
![image](https://github.com/meteorchenwu/meteor/blob/chenwu/mc/src/main/webapp/img/demo/result.png)
