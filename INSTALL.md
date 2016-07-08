流星实时数据开发平台安装说明
-------------

1、安装好如下服务：<br />
必备：mysql，redis集群，kafka集群，spark集群。<br />
可选：cassandra集群（涉及超大量级去重、join才需要用到，如基于历史数据算新UV，join成为新用户对应的来源渠道数据）<br /><br />

2、下载该平台源码，在你的mysql中执行doc/sql/create.sql。<br /><br />

3、将dao/src/main/resources/meteor-app.properties的内容，改为你的mysql连接信息。<br /><br />

4、执行mvn clean install -Dmaven.test.skip=true，打包。<br /><br />

5、取jetty-server和mc模块在target目录下的包，执行如下命令，就可以通过http://x.x.x.x:8080 ，进入前台管理系统<br />
java -Xms128m -Xmx128m -cp /xx/xx/meteor-jetty-server-1.0-SNAPSHOT-jar-with-dependencies.jar com.meteor.jetty.server.JettyServer "/xx/xx/meteor-mc-1.0-SNAPSHOT.war" "/" "8080" > mc.log 2>&1 & <br /><br />

待续...









