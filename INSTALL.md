安装并用一台机器运行demo示例说明
====================================

##目录
* [创建spark用户和ssh无密码登录](#创建spark用户和ssh无密码登录)
* [java](#java)
* [安装spark](#安装spark)

创建spark用户和ssh无密码登录
---------------------
<pre>
sudo -s su - root
useradd -s /bin/bash -p 123456789 -m spark -d /data/spark
mkdir -p /data/apps/meteor
chown -R spark:spark /data/apps

sudo -s su - spark
ssh-keygen -t rsa（一直按空格键）

cat /data/spark/.ssh/id_rsa.pub >> /data/spark/.ssh/authorized_keys
</pre>

java
---------------------
<pre>安装Java HotSpot 1.7</pre>

安装spark
---------------------
##### 1、下载spark：http://www.apache.org/dyn/closer.lua/spark/spark-1.6.2/spark-1.6.2-bin-hadoop2.4.tgz
##### 2、cd /data/apps/，把包放到这个目录下，解压：tar -zxvf spark-1.6.2-bin-hadoop2.4.tgz
##### 3、ln -s spark-1.6.2-bin-hadoop2.4 spark
##### 4、cd /data/apps/spark/conf
##### 5、cp slaves.template slaves
##### 6、cp log4j.properties.template log4j.properties，并修改成如下:

<pre>
log4j.rootCategory=WARN, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Settings to quiet third party logs that are too verbose
log4j.logger.org.spark-project.jetty=WARN
log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=WARN
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=WARN
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR

log4j.logger.com.meteor=INFO
</pre>

##### 7、mkdir -p /data/apps/spark/work/
##### 8、cp spark-env.sh.template spark-env.sh，并修改成如下:

<pre>
export SPARK_DAEMON_MEMORY=512m
export JAVA_HOME=/usr/local/jdk
export SPARK_HOME=/data/apps/spark

export SPARK_WORKER_CORES=60
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_DIR=$SPARK_HOME/work

export SPARK_LOCAL_DIRS=/tmp
</pre>

##### 9、cp spark-defaults.conf.template spark-defaults.conf
##### 10、启动spark集群：/data/apps/spark/sbin/start-all.sh，通过http://本机外网IP:8080
##### 11、关闭spark集群：/data/apps/spark/sbin/stop-all.sh

三、安装kafka
1、下载kafka：https://www.apache.org/dyn/closer.cgi?path=/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz
2、cd /data/apps/，把包放到这个目录下，解压：tar -zxvf kafka_2.10-0.8.2.0.tgz
3、ln -s kafka_2.10-0.8.2.0 kafka
4、vim /data/apps/kafka/conf/server.properties，增加配置auto.create.topics.enable=true
5、启动zookeeper:
/data/apps/kafka/bin/zookeeper-server-start.sh /data/apps/kafka/config/zookeeper.properties > /tmp/startup_zookeeper.log 2>&1 &
6、启动kafka：
/data/apps/kafka/bin/kafka-server-start.sh /data/apps/kafka/config/server.properties > /tmp/startup_kafka.log 2>&1 &

四、安装redis集群
1、下载redis-3.0.7：http://redis.io/download
2、找一个临时目录，解压：tar -zxvf redis-3.0.7.tar.gz
3、cd redis-3.0.7，执行make命令
4、
mkdir -p /data/apps/redis-3.0.7/conf 
mkdir -p /data/apps/redis-3.0.7/bin 
mkdir -p /data/apps/redis-3.0.7/data
ln -s /data/apps/redis-3.0.7 /data/apps/redis
cp redis.conf sentinel.conf /data/apps/redis/conf
cp runtest* /data/apps/redis/bin
cd src
cp mkreleasehdr.sh redis-benchmark redis-check-aof redis-check-dump redis-cli redis-sentinel redis-server redis-trib.rb /data/apps/redis/bin/
5、
cp /data/apps/redis/conf/redis.conf /data/apps/redis/conf/redis-6379.conf
vim /data/apps/redis/conf/redis-6379.conf
<pre>
daemonize yes
pidfile /var/run/redis-6379.pid
port 6379

#save 900 1
#save 300 10
#save 60 10000

dbfilename dump-6379.rdb
dir /data/apps/redis/data

maxmemory 1g
maxmemory-policy allkeys-lru
maxmemory-samples 3

cluster-enabled yes
cluster-config-file /data/apps/redis/conf/nodes-6379.conf
</pre>

cp /data/apps/redis/conf/redis.conf /data/apps/redis/conf/redis-6380.conf
vim /data/apps/redis/conf/redis-6380.conf
<pre>
daemonize yes
pidfile /var/run/redis-6380.pid
port 6380

#save 900 1
#save 300 10
#save 60 10000

dbfilename dump-6380.rdb
dir /data/apps/redis/data

maxmemory 1g
maxmemory-policy allkeys-lru
maxmemory-samples 3

cluster-enabled yes
cluster-config-file /data/apps/redis/conf/nodes-6380.conf
</pre>


cp /data/apps/redis/conf/redis.conf /data/apps/redis/conf/redis-6381.conf
vim /data/apps/redis/conf/redis-6381.conf
<pre>
daemonize yes
pidfile /var/run/redis-6381.pid
port 6380

#save 900 1
#save 300 10
#save 60 10000

dbfilename dump-6381.rdb
dir /data/apps/redis/data

maxmemory 1g
maxmemory-policy allkeys-lru
maxmemory-samples 3

cluster-enabled yes
cluster-config-file /data/apps/redis/conf/nodes-6381.conf
</pre>

6、
/data/apps/redis/bin/redis-server /data/apps/redis/conf/redis-6379.conf
/data/apps/redis/bin/redis-server /data/apps/redis/conf/redis-6380.conf
/data/apps/redis/bin/redis-server /data/apps/redis/conf/redis-6381.conf

7、
exit
sudo -s su - root
apt-get update
apt-get install ruby1.9.3
apt-get install rubygems
gem install redis

8、
sudo -s su - spark
/data/apps/redis/bin/redis-trib.rb create 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381

五、安装cassandra
可选，涉及超大量级去重、join才需要用到，如基于历史数据算新UV，join成为新用户对应的来源渠道数据

六、配置host
sudo -s su - root
vim /etc/hosts

127.0.0.1 kafka1<br />
127.0.0.1 redis1<br />
127.0.0.1 cassandra1<br /><br />

2、下载该平台源码，假设本地路径为：/data/meteor，在你的mysql中执行如下sql脚本<br />
/data/meteor/doc/sql/create.sql<br />
/data/meteor/doc/sql/init_demo.sql<br /><br />

3、将/data/meteor/dao/src/main/resources/meteor-app.properties的内容，改为你的mysql连接信息。<br /><br />

4、执行mvn clean install -Dmaven.test.skip=true，打包。<br /><br />
其中下载scala会很慢，因为是在国外的，可以从http://pan.baidu.com/s/1bpxBhrL这里下载scala的包，解压到你的maven respository/org/目录下

5、启动前台管理系统程序，通过http://x.x.x.x:8070 登录<br />
java -Xms128m -Xmx128m -cp /data/meteor/jetty-server/target/meteor-jetty-server-1.0-SNAPSHOT-jar-with-dependencies.jar com.meteor.jetty.server.JettyServer "/data/meteor/mc/target/meteor-mc-1.0-SNAPSHOT.war" "/" "8070" > mc.log 2>&1 & <br /><br />

6、启动模拟源头数据程序<br />
java -Xms128m -Xmx128m -cp /data/meteor/demo/target/meteor-demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.meteor.demo.DemoSourceData <br /><br />

7、启动后台实时计算程序<br />
1)按需修改/data/meteor/conf/meteor.properties<br />
2)cp /data/meteor/hiveudf/target/meteor-hiveudf-1.0-SNAPSHOT-jar-with-dependencies.jar /data/spark_lib_ext/<br />
3)cp /data/meteor/conf/log4j.properties /data/apps/spark/conf/<br />
4)vim /data/apps/spark/conf/spark-default.conf<br />
<pre>
spark.driver.extraClassPath  /data/spark_lib_ext/*
spark.executor.extraClassPath  /data/spark_lib_ext/*
</pre>
5)启动程序
<pre>
/data/apps/spark/bin/spark-submit \
  --class com.meteor.server.MeteorServer \
  --master spark://127.0.0.1:7077 \
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
java -Xms128m -Xmx128m -cp /data/meteor/jetty-server/target/meteor-jetty-server-1.0-SNAPSHOT-jar-with-dependencies.jar com.meteor.jetty.server.JettyServer "/data/meteor/transfer/target/meteor-transfer-1.0-SNAPSHOT.war" "/" "8090" > transfer.log 2>&1 & <br /><br />










