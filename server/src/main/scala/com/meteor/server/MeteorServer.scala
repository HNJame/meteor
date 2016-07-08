package com.meteor.server

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.UUID

import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.reflect.runtime.universe

import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.math.NumberUtils
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils

import com.meteor.model.view.importqueue.ImportKafkaTask
import com.meteor.server.context.CassandraContextSingleton
import com.meteor.server.context.ExecutorContext
import com.meteor.server.cron.CronTaskLoader
import com.meteor.server.executor.instance.InstanceFlowExecutor
import com.meteor.server.factory.DefTaskFactory
import com.meteor.server.factory.InstanceFlowExecutorObjectPool
import com.meteor.server.factory.TaskThreadPoolFactory
import com.meteor.server.util.LocalCacheUtil
import com.meteor.server.util.Logging
import com.meteor.server.util.PropertiesUtil
import com.meteor.server.util.RedisClusterUtil

import kafka.serializer.StringDecoder

object MeteorServer extends Logging {

  def main(args: Array[String]) {
    logInfo("Starting MeteorServer!")
    if (args.length != 1) {
      System.err.println("MeterorServer <PropertiesFile>");
      System.exit(1);
    }
    PropertiesUtil.load(args(0))

    ExecutorContext.execCronTaskOnStartup = PropertiesUtil.get("meteor.execCronTaskOnStartup", "false")

    val sparkConf = new SparkConf().setAppName(PropertiesUtil.get("meteor.appName", "MeteorServer"))
    val patchSecond = Integer.parseInt(PropertiesUtil.get("meteor.patchSecond", "60"))

    val streamContext = new StreamingContext(sparkConf, Seconds(patchSecond))
    ExecutorContext.streamContext = streamContext
    val sparkContext = streamContext.sparkContext
    val hiveContext = new HiveContext(sparkContext)
    ExecutorContext.hiveContext = hiveContext

    ExecutorContext.jdbcDriver = PropertiesUtil.get("meteor.jdbc.driver")
    ExecutorContext.jdbcUrl = PropertiesUtil.get("meteor.jdbc.url")
    ExecutorContext.jdbcUsername = PropertiesUtil.get("meteor.jdbc.username")
    ExecutorContext.jdbcPassword = PropertiesUtil.get("meteor.jdbc.password")

    ExecutorContext.sshExecCronTaskMachines = StringUtils.split(PropertiesUtil.get("meteor.sshExecCronTaskMachines"), ",")
    ExecutorContext.cronTaskExecJar = PropertiesUtil.get("meteor.cronTaskExecJar")
    ExecutorContext.cronTaskLogPath = PropertiesUtil.get("meteor.cronTaskLogPath")

    DefTaskFactory.startup()
    CronTaskLoader.startup()

    regUDF(hiveContext)
    importQueue()

    streamContext.start()
    logInfo("Finished startup")
    streamContext.awaitTermination()
  }

  def importQueue(): Unit = {
    for (sourceTaskId: Integer <- DefTaskFactory.defAllValid.getImportQueueSet.toSet) {
      val sourceTaskIdStr = sourceTaskId.toString()
      val task = DefTaskFactory.getCloneById(sourceTaskId).asInstanceOf[ImportKafkaTask]
      if (!StringUtils.equals(System.getenv("DWENV"), "prod")) {
        task.setBrokers(ExecutorContext.kafkaClusterHostPorts)
      }
      logInfo(s"Startup sourceTask : ${task.getFileId}, ${task.getFileName}, ${task.getTopics}")
      val kafkaParams = Map[String, String]("metadata.broker.list" -> task.getBrokers, "group.id" -> task.getGroupId)
      val topicSet = scala.collection.mutable.Set[String]()
      for (topic: String <- StringUtils.split(task.getTopics, ",")) {
        topicSet += StringUtils.trim(topic)
      }
      //        ExecutorContext.streamContext.sparkContext.setLocalProperty("spark.scheduler.pool", task.getPriority.toString())
      val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ExecutorContext.streamContext, kafkaParams, topicSet.toSet)
      //      stream.checkpoint(Duration(9000))
      var streamRe = stream.transform({ rdd =>
        TaskThreadPoolFactory.cachedThreadPool.submit(new Runnable() {
          override def run(): Unit = {
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges.sortBy(x => x.partition)
            var str = "\n"
            for (o <- offsetRanges) {
              str += s"${o.topic}    ${o.partition}    ${o.fromOffset}    ${o.untilOffset}\n"
            }
            logInfo(str)
          }
        })
        rdd
      })
      if (task.getRePartitionNum > 0) {
        streamRe = streamRe.repartition(task.getRePartitionNum)
      }
      streamRe.map(_._2).foreachRDD((rdd: RDD[String], time: Time) => {
        TaskThreadPoolFactory.cachedThreadPool.submit(new Runnable() {
          override def run(): Unit = {
            if (!rdd.isEmpty()) {
              val instanceFlowExecutor = InstanceFlowExecutorObjectPool.getPool(sourceTaskId).borrowObject().asInstanceOf[InstanceFlowExecutor]
              InstanceFlowExecutorObjectPool.getPool(sourceTaskId).invalidateObject(instanceFlowExecutor)
              val paramMap = Map[String, Any]("rdd" -> rdd)
              instanceFlowExecutor.startup(sourceTaskId, paramMap)
            }
          }
        })
      })
    }
  }

  //$SPARK_HOME/con/spark-default
  //spark.driver.extraClassPath  /home/spark/spark/lib_ext/*
  //spark.executor.extraClassPath  /home/spark/spark/lib_ext/*
  def regUDF(hiveContext: HiveContext): Unit = {
    logInfo("Startup regUDF")

    hiveContext.sql("create temporary function c_json_tuple as 'com.duowan.hive.udf.udtf.CustomGenericUDTFJSONTuple'")

    hiveContext.udf.register("c_uuid", () => {
      UUID.randomUUID().toString()
    })

    hiveContext.udf.register("json_2map", (jsonStr: String) => {
      var result = Map[String, String]()
      if (StringUtils.isNotBlank(jsonStr)) {
        val resultMap = com.alibaba.fastjson.JSON.parseObject(jsonStr, new com.alibaba.fastjson.TypeReference[java.util.Map[String, String]]() {})
        result = resultMap.toMap
      }
      result
    })

    hiveContext.udf.register("merge_2map", (map1: Map[String, String], map2: Map[String, String]) => {
      var result = scala.collection.mutable.Map[String, String]();
      if (map1 != null) {
        result.putAll(map1)
      }
      if (map2 != null) {
        result.putAll(map2)
      }
      result
    })

    hiveContext.udf.register("struct_2map", (data: GenericRowWithSchema) => {
      var result = scala.collection.mutable.Map[String, String]();
      if (data != null) {
        val fieldNames = data.schema.fieldNames
        var i = 0
        for (fieldName <- fieldNames) {
          val d = data.get(i)
          if (d != null) {
            result += fieldName -> d.toString
          }
          i += 1
        }
      }
      result
    })

    hiveContext.udf.register("c_join", (table: String, toCassandra: Boolean, useLocalCache: Boolean, cassandraExpireSeconds: Integer, redisExpireSeconds: Integer, partition: String, key: String) => {
      var result = "{}"
      if (StringUtils.isNotBlank(key)) {
        var parKey = key
        if (StringUtils.isNotBlank(partition)) {
          parKey = partition + "|" + key
        }

        val dataKey = table + "|" + parKey
        var continueFlag = true
        if (useLocalCache) {
          val localCacheResult = LocalCacheUtil.get(dataKey)
          if (StringUtils.isNotBlank(localCacheResult)) {
            result = localCacheResult
            continueFlag = false
          }
        }

        if (continueFlag) {
          val redisResult = RedisClusterUtil.get(dataKey)
          if (StringUtils.isNotBlank(redisResult)) {
            result = redisResult
            continueFlag = false
            if (useLocalCache) LocalCacheUtil.put(dataKey, redisResult)
          }
        }

        if (continueFlag && toCassandra) {
          val session = CassandraContextSingleton.getSession()
          val tablePS = CassandraContextSingleton.getPreparedStatement(s"SELECT value FROM $table WHERE key=?", table, cassandraExpireSeconds)
          val tableRS = session.execute(tablePS.bind(parKey))
          if (tableRS != null) {
            val tableRSList = tableRS.toList
            if (tableRSList != null && tableRSList.size > 0) {
              val jsonData = tableRSList(0).getString("value")
              if (StringUtils.isNotBlank(jsonData)) {
                result = jsonData
                if (useLocalCache) LocalCacheUtil.put(dataKey, jsonData)
                RedisClusterUtil.setex(dataKey, jsonData, redisExpireSeconds)
              }
            }
          }
        }
      }
      result
    })

    hiveContext.udf.register("c_distinct", (table: String, toCassandra: Boolean, cassandraExpireSeconds: Integer, redisExpireSeconds: Integer, partition: String, key: String, value: String) => {
      var result = false
      if (StringUtils.isNotBlank(key)) {
        var parKey = key
        if (StringUtils.isNotBlank(partition)) {
          parKey = partition + "|" + key
        }

        val dataKey = table + "|" + parKey
        val localCacheResult = LocalCacheUtil.get(dataKey)
        if (localCacheResult == null) {
          LocalCacheUtil.put(dataKey, value)
          if (!toCassandra) {
            result = RedisClusterUtil.setneex(dataKey, value, redisExpireSeconds)
          } else {
            val redisResult = RedisClusterUtil.exists(dataKey)
            if (!redisResult) {
              RedisClusterUtil.setex(dataKey, value, redisExpireSeconds)
              val session = CassandraContextSingleton.getSession()
              val tablePS = CassandraContextSingleton.getPreparedStatement(s"INSERT INTO $table (key, value) VALUES (?, ?) IF NOT EXISTS", table, cassandraExpireSeconds)
              val tableRS = session.execute(tablePS.bind(parKey, value))
              if (tableRS != null) {
                val tableRSOne = tableRS.one
                if (tableRSOne != null && tableRSOne.getBool(0)) {
                  result = true
                }
              }
            }
          }
        }
      }
      result
    })

    hiveContext.udf.register("c_sum", (table: String, partition: String, key: String, value: Long, redisExpireSeconds: Integer) => {
      var tablePartition = table
      if (StringUtils.isNotBlank(partition)) {
        tablePartition = table + "_" + partition
      }
      RedisClusterUtil.hincrBy(tablePartition, key, value, redisExpireSeconds)
    })

    hiveContext.udf.register("get_sum", (table: String, partition: String, key: String) => {
      var tablePartition = table
      if (StringUtils.isNotBlank(partition)) {
        tablePartition = table + "_" + partition
      }
      RedisClusterUtil.hget(tablePartition, key)
    })

    hiveContext.udf.register("get_slot_time", (time: String, slot: Integer, sDateFormat: String, tDateFormat: String) => {
      val date = DateUtils.parseDate(time, sDateFormat)
      val calendar = Calendar.getInstance()
      calendar.setTime(date)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
      val minuteSlot = (calendar.get(Calendar.MINUTE) / slot) * slot
      calendar.set(Calendar.MINUTE, minuteSlot)
      DateFormatUtils.format(calendar, tDateFormat)
    })

    hiveContext.udf.register("urlDecode", (url: String) => {
      var result = url
      if (StringUtils.isNotBlank(url)) {
        try {
          result = URLDecoder.decode(url, "UTF-8")
        } catch {
          case e: Exception => {}
        }
      }
      result
    })

    hiveContext.udf.register("period_online_days", (stime: String, days: String, period: Int, sDateFormat: String, tDateFormat: String) => {
      val sTimeDF = new SimpleDateFormat(sDateFormat)
      val sdf = new SimpleDateFormat(tDateFormat)
      var day = new Date;
      try {
        day = DateUtils.parseDate(stime, sDateFormat);
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      if (StringUtils.isBlank(days)) {
        val today = sdf.format(day)
        today
      } else {
        val today = sdf.format(day)
        if (days.endsWith(today)) {
          days
        } else {
          var loginDays = days.concat(",").concat(today).split(",")
          val lastPeriStartDate = DateUtils.addDays(new Date(), period * (-1))
          val lastPeriStartDay = sdf.format(lastPeriStartDate)

          loginDays = loginDays.filter { x => lastPeriStartDay.compareTo(x) < 0 }
          val result = loginDays.reduce((fir: Any, sec: Any) => { fir.asInstanceOf[String].+(",").+(sec.asInstanceOf[String]) })
          result
        }
      }
    })

    hiveContext.udf.register("c_max", (table: String, partition: String, value: Long, redisExpireSeconds: Integer) => {
      var tablePartition = table
      if (StringUtils.isNotBlank(partition)) {
        tablePartition = table + "_" + partition
      }
      RedisClusterUtil.max(tablePartition, value, redisExpireSeconds)
    })

    hiveContext.udf.register("c_min", (table: String, partition: String, value: Long, redisExpireSeconds: Integer) => {
      var tablePartition = table
      if (StringUtils.isNotBlank(partition)) {
        tablePartition = table + "_" + partition
      }
      RedisClusterUtil.min(tablePartition, value, redisExpireSeconds)
    })
  }

}