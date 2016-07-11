流星实时数据开发平台安装，并运行demo示例说明
-------------

1、安装并启动好如下服务：<br />
必备：mysql，redis集群，kafka集群，spark集群。<br />
可选：cassandra集群（涉及超大量级去重、join才需要用到，如基于历史数据算新UV，join成为新用户对应的来源渠道数据）<br /><br />

备注：kafka集群设置成auto.create.topics.enable=true<br />
版本：<br />
spark-1.6.2-bin-hadoop2.4<br />
redis-3.0.7<br />
kafka_2.10-0.8.2.0<br />
dsc-cassandra-2.1.7<br /><br />

并在所有机器做host:<br />
x.x.x.x kafka1<br />
x.x.x.x kafka2<br />
x.x.x.x kafka3<br />
x.x.x.x redis1<br />
x.x.x.x redis2<br />
x.x.x.x redis3<br />
x.x.x.x cassandra1<br />
x.x.x.x cassandra2<br />
x.x.x.x cassandra3<br /><br />

2、下载该平台源码，假设本地路径为：/data/meteor，在你的mysql中执行如下sql脚本<br />
/data/meteor/doc/sql/create.sql<br />
/data/meteor/doc/sql/init_demo.sql<br /><br />

3、将/data/meteor/dao/src/main/resources/meteor-app.properties的内容，改为你的mysql连接信息。<br /><br />

4、执行mvn clean install -Dmaven.test.skip=true，打包。<br /><br />

5、启动前台管理系统程序，通过http://x.x.x.x:8070 登录<br />
java -Xms128m -Xmx128m -cp /data/meteor/jetty-server/target/meteor-jetty-server-1.0-SNAPSHOT-jar-with-dependencies.jar com.meteor.jetty.server.JettyServer "/data/meteor/mc/target/meteor-mc-1.0-SNAPSHOT.war" "/" "8070" > mc.log 2>&1 & <br /><br />

6、启动模拟源头数据程序<br />
java -Xms128m -Xmx128m -cp /data/meteor/demo/target/meteor-demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.meteor.demo.DemoSourceData <br /><br />

7、启动后台实时计算程序<br />
1)按需修改/data/meteor/conf/meteor.properties<br />
2)将/data/meteor/hiveudf/target/meteor-hiveudf-1.0-SNAPSHOT-jar-with-dependencies.jar，复制到spark集群每台机器/data/spark_lib_ext/下<br />
3)将/data/meteor/conf/log4j.properties，复制到spark集群每台机器:spark安装目录/conf/，下
3)编辑：spark安装目录/conf/spark-default.conf，加入如下配置<br />
<pre>
spark.driver.extraClassPath  /data/spark_lib_ext/*
spark.executor.extraClassPath  /data/spark_lib_ext/*
</pre>
4)启动程序
<pre>
spark安装目录/bin/spark-submit \
  --class com.meteor.server.MeteorServer \
  --master spark://spark主节点IP:7077 \
  --executor-memory 1G \
  --total-executor-cores 16 \
  --driver-cores 4 \
  --driver-memory 1G \
  --supervise \
  --verbose \
  /data/meteor/server/target/meteor-server-1.0-SNAPSHOT-jar-with-dependencies.jar \
  "/data/meteor/conf/meteor.properties"
</pre>
<br />

8、启动如下程序，用于把执行日志导回mysql，方便前台管理系统查看<br />
java -Xms128m -Xmx128m -cp /data/meteor/jetty-server/target/meteor-jetty-server-1.0-SNAPSHOT-jar-with-dependencies.jar com.meteor.jetty.server.JettyServer "/data/meteor/transfer/target/meteor-transfer-1.0-SNAPSHOT.war" "/" "8080" > transfer.log 2>&1 & <br /><br />










