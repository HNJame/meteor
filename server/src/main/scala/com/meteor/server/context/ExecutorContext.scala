package com.meteor.server.context

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext

import com.meteor.server.util.PropertiesUtil

object ExecutorContext {

  init()

  var appName: String = _
  var patchSecond: Int = _
  var execCronTaskOnStartup: String = _
  
  var kafkaClusterHostPorts: String = _
  var cassandraClusterHosts: String = _
  var redisClusterHostPorts: String = _
  var redisMaxTotals: Int = _
  var redisMaxIdle: Int = _
  var zkConnection:String=_
  val appCloseZkNamesparce:String="appCloseListener"

  var jdbcDriver: String = _
  var jdbcUrl: String = _
  var jdbcUsername: String = _
  var jdbcPassword: String = _
  
  var sshExecCronTaskMachines: Array[String] = _
  var cronTaskExecJar: String = _
  var cronTaskLogPath: String = _

  var hiveContext: HiveContext = _
  var streamContext: StreamingContext = _

  val instanceTaskTopic = "instance_task"
  val instanceFlowTopic = "instance_flow"
  val performanceTopic = "performance"

  var excludeTaskIds: Array[Int] = _


  val topicAndPartitions=new ConcurrentHashMap[String,String]()


  def init(): Unit = {
    PropertiesUtil.load("/data/apps/spark/conf/meteor.properties")

    appName = PropertiesUtil.get("meteor.appName", "MeteorServer")
    patchSecond = Integer.parseInt(PropertiesUtil.get("meteor.patchSecond", "60"))
    execCronTaskOnStartup = PropertiesUtil.get("meteor.execCronTaskOnStartup", "false")
    
    kafkaClusterHostPorts = PropertiesUtil.get("meteor.kafkaClusterHostPorts", "kafka1:9092,kafka2:9092,kafka3:9092")
    cassandraClusterHosts = PropertiesUtil.get("meteor.cassandraClusterHosts", "cassandra1,cassandra2,cassandra3")
    redisClusterHostPorts = PropertiesUtil.get("meteor.redisClusterHostPorts", "redis1:6379,redis2:6379,redis3:6379")
    redisMaxTotals = Integer.parseInt(PropertiesUtil.get("meteor.redisMaxTotals", "600"))
    redisMaxIdle = Integer.parseInt(PropertiesUtil.get("meteor.redisMaxIdle", "600"))
    zkConnection=PropertiesUtil.get("meteor.zookeeper")

    jdbcDriver = PropertiesUtil.get("meteor.jdbc.driver")
    jdbcUrl = PropertiesUtil.get("meteor.jdbc.url")
    jdbcUsername = PropertiesUtil.get("meteor.jdbc.username")
    jdbcPassword = PropertiesUtil.get("meteor.jdbc.password")

    sshExecCronTaskMachines = StringUtils.split(PropertiesUtil.get("meteor.sshExecCronTaskMachines"), ",")
    cronTaskExecJar = PropertiesUtil.get("meteor.cronTaskExecJar")
    cronTaskLogPath = PropertiesUtil.get("meteor.cronTaskLogPath")

    val excludeTaskIdsStr = PropertiesUtil.get("meteor.excludeTaskIds")
    if (StringUtils.isNotBlank(excludeTaskIdsStr)) {
      excludeTaskIds = StringUtils.split(excludeTaskIdsStr, ",").map { x => Integer.parseInt(StringUtils.trim(x)) }
    } else {
      excludeTaskIds = Array[Int]()
    }
  }
}