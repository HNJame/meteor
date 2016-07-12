问题
===================

#### 1、重启数据丢失
问题：<br />
后台实时计算程序server重启后，会从kafka最后的位置消费数据，关闭到重新启动那一段时间的数据会丢失<br /><br />
解决办法：<br />
自己弄一个中转程序：<br />
源头数据流-> 中转程序(里面设一个开关，还能做一些如转json字符串逻辑等) -> 发至kafka新的topic -> spark-stream消费<br />
先把开关停掉，等kafka新的topic的数据被spark消费处理完，再重启，并打开开关

#### 2、统计结果落地
目前整个平台的统计结果，都是落地至kafka<br />
用户可以从kafka拉数据，定制自己的落地方案程序，如落地至mysql、elasticsearch等

#### 3、hive concat函数陷阱
concat(a, b, c)，a、b、c中任何一个为null对象，则结果返回null<br />
建议用户在中间表的清洗逻辑中，加上COALESCE(a, 'null')清洗逻辑
