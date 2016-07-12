package com.meteor.server.context

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.broadcast.Broadcast

object ExecutorContext extends Serializable {

	val cassandraClusterHosts = "cassandra1"
  val redisClusterHostPorts = "redis1:6379"
  val kafkaClusterHostPorts = "kafka1:9092"
  
  var jdbcDriver: String = _
  var jdbcUrl: String = _
  var jdbcUsername: String = _
  var jdbcPassword: String = _
  
  var sshExecCronTaskMachines: Array[String] = _
  var cronTaskExecJar: String = _
  var cronTaskLogPath: String = _

  var execCronTaskOnStartup: String = _
  
  @transient var hiveContext: HiveContext = _

  @transient var streamContext: StreamingContext = _

  val instanceTaskTopic = "instance_task"
  val instanceFlowTopic = "instance_flow"
  val performanceTopic = "performance"

}