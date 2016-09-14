package com.meteor.server

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.UUID

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata

import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap

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
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

import com.meteor.model.view.importqueue.ImportKafkaTask
import com.meteor.server.context.{ CassandraContextSingleton, ExecutorContext}
import com.meteor.server.cron.CronTaskLoader
import com.meteor.server.executor.instance.InstanceFlowExecutor
import com.meteor.server.factory.DefTaskFactory
import com.meteor.server.factory.InstanceFlowExecutorObjectPool
import com.meteor.server.factory.TaskThreadPoolFactory
import com.meteor.server.util._
import com.meteor.server.checkpoint.ZkOffsetCheckPoint

import kafka.serializer.StringDecoder

import scala.collection.mutable

object MeteorServer extends Logging {

  def main(args: Array[String]) {
    logInfo("Starting MeteorServer!")

    val sparkConf = new SparkConf().setAppName(ExecutorContext.appName)
    val streamContext = new StreamingContext(sparkConf, Seconds(ExecutorContext.patchSecond))
    ExecutorContext.streamContext = streamContext
    val hiveContext = new HiveContext(streamContext.sparkContext)
    ExecutorContext.hiveContext = hiveContext

    val offsetCheckpointListener=new ZkOffsetCheckPoint
    offsetCheckpointListener.registerCheckpointListener(streamContext)

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
      ExecutorContext.streamContext.sparkContext.setLocalProperty("spark.scheduler.pool", task.getPriority.toString())

      modifyCheckpointOffsets(task,topicSet)

      var topicAndPartitionMap= scala.collection.immutable.Map[TopicAndPartition,Long]();
      for((topicParStr,offset)<-ExecutorContext.topicAndPartitions){
        val topicParArray=topicParStr.split(":")
        if (topicSet.contains(topicParArray(0))) {
          topicAndPartitionMap += new TopicAndPartition(topicParArray(0), topicParArray(1).toInt) -> offset.toLong
        }
      }

      var streamRe = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ExecutorContext.streamContext, kafkaParams, topicAndPartitionMap,
        (mmd: MessageAndMetadata[String, String]) => mmd.message()).transform({ rdd =>
        TaskThreadPoolFactory.cachedThreadPool.submit(new Runnable() {
          override def run(): Unit = {
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges.sortBy(x => x.partition)
            var str = "\n"
            for (o <- offsetRanges) {
              str += s"${o.topic}    ${o.partition}    ${o.fromOffset}    ${o.untilOffset}  ${o.untilOffset - o.fromOffset}\n"
              ExecutorContext.topicAndPartitions.put(s"${o.topic}:${o.partition}", o.untilOffset.toString)
            }
            logInfo(str)
          }
        })
        rdd
      })

      if (task.getRePartitionNum > 0) {
        streamRe = streamRe.repartition(task.getRePartitionNum)
      }
      streamRe.foreachRDD((rdd: RDD[String], time: Time) => {
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

  //修改offset，例如offset越界或者partition增加减少等，都有必要修改
  //offset越界的时候，直接去最大的offset
  def modifyCheckpointOffsets(task :ImportKafkaTask,set:scala.collection.mutable.Set[String]): Unit ={
    import scala.collection.JavaConverters._
    val offsetTool=OffsetTool.getInstance()
    val earliestOffsets=offsetTool.getEarliestOffset(task.getBrokers,set.toList.asJava,task.getGroupId).asScala
    val latestOffsets=offsetTool.getLastOffset(task.getBrokers,set.toList.asJava,task.getGroupId).asScala
    val earliestOffsetSet=new mutable.HashMap[String,Long]()
    for ((topicAndPartition,offset)<-earliestOffsets){
      earliestOffsetSet+=topicAndPartition.topic+":"+topicAndPartition.partition->offset
    }

    val latestOffsetSet=new mutable.HashMap[String,Long]()
    for ((topicAndPartition,offset)<-latestOffsets){
      latestOffsetSet+=topicAndPartition.topic+":"+topicAndPartition.partition->offset
    }
    assert(latestOffsetSet.size==earliestOffsetSet.size)

    for ((topicAndPartition,offset)<-latestOffsetSet){
      if(ExecutorContext.topicAndPartitions.containsKey(topicAndPartition)){
        val parOffset=ExecutorContext.topicAndPartitions.get(topicAndPartition).toLong
        if (parOffset > offset || parOffset < earliestOffsetSet.get(topicAndPartition).get){
          ExecutorContext.topicAndPartitions+=topicAndPartition->latestOffsetSet.get(topicAndPartition).get.toString
          logInfo(s"the checkpoint offset[${parOffset}}] of [${topicAndPartition}] out of range ${earliestOffsetSet.get(topicAndPartition).get}--${offset} " +
            s"use max offset replace")
        }else{
          logInfo(s"the checkpoint offset of [${topicAndPartition}] is [${parOffset}}]!")
        }
      }else{
        ExecutorContext.topicAndPartitions+=topicAndPartition->offset.toString
        logInfo(s"[${topicAndPartition}] has not checkpoint offset,use max offset[${offset.toString}] replace")
      }
    }



  }



  //$SPARK_HOME/con/spark-default
  //spark.driver.extraClassPath  /home/spark/spark/lib_ext/*
  //spark.executor.extraClassPath  /home/spark/spark/lib_ext/*
  def regUDF(hiveContext: HiveContext): Unit = {
    logInfo("Startup regUDF")

    hiveContext.sql("create temporary function c_json_tuple as 'com.meteor.hive.udf.udtf.CustomGenericUDTFJSONTuple'")

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

    hiveContext.udf.register("c_join", (table: String, useLocalCache: Boolean, cacheEmpty: Boolean, useRedis: Boolean, redisExpireSeconds: Integer, useCassandra: Boolean, cassandraExpireSeconds: Integer, partition: String, key: String) => {
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

        if (continueFlag && useRedis) {
          val redisResult = RedisClusterUtil.get(dataKey)
          if (StringUtils.isNotBlank(redisResult)) {
            result = redisResult
            continueFlag = false
            if (useLocalCache) LocalCacheUtil.put(dataKey, redisResult)
          } else if (!useCassandra && useLocalCache && cacheEmpty) {
            LocalCacheUtil.put(dataKey, "{}")
          }
        }

        if (continueFlag && useCassandra) {
          val session = CassandraContextSingleton.getSession()
          val tablePS = CassandraContextSingleton.getPreparedStatement(s"SELECT value FROM $table WHERE key=?", table, cassandraExpireSeconds)
          val tableRS = session.execute(tablePS.bind(parKey))
          var isNotExists = true
          if (tableRS != null) {
            val tableRSList = tableRS.toList
            if (tableRSList != null && tableRSList.size > 0) {
              val jsonData = tableRSList(0).getString("value")
              if (StringUtils.isNotBlank(jsonData)) {
                isNotExists = false
                result = jsonData
                if (useLocalCache) LocalCacheUtil.put(dataKey, jsonData)
                if (useRedis) RedisClusterUtil.setex(dataKey, jsonData, redisExpireSeconds)
              }
            }
          }
          if (isNotExists && useLocalCache && cacheEmpty) {
            LocalCacheUtil.put(dataKey, "{}")
          }
        }
      }
      result
    })

    hiveContext.udf.register("c_distinct", (table: String, redisExpireSeconds: Integer, useCassandra: Boolean, cassandraExpireSeconds: Integer, partition: String, key: String, value: String) => {
      var result = 0L
      if (StringUtils.isNotBlank(key)) {
        var parKey = key
        if (StringUtils.isNotBlank(partition)) {
          parKey = partition + "|" + key
        }

        val dataKey = table + "|" + parKey
        val localCacheResult = LocalCacheUtil.get(dataKey)
        if (localCacheResult == null) {
          LocalCacheUtil.put(dataKey, "")
          if (!useCassandra) {
            result = RedisClusterUtil.setneex(dataKey, value, redisExpireSeconds)
          } else {
            val redisResult = RedisClusterUtil.setneex(dataKey, "", redisExpireSeconds)
            if (redisResult == 1L) {
              val session = CassandraContextSingleton.getSession()
              val tablePS = CassandraContextSingleton.getPreparedStatement(s"INSERT INTO $table (key, value) VALUES (?, ?) IF NOT EXISTS", table, cassandraExpireSeconds)
              val tableRS = session.execute(tablePS.bind(parKey, value))
              if (tableRS != null) {
                val tableRSOne = tableRS.one
                if (tableRSOne != null && tableRSOne.getBool(0)) {
                  result = 1L
                }
              }
            }
          }
        }
      }
      result
    })

    hiveContext.udf.register("get_sum", (table: String, partition: String, key: String) => {
      val tablePartition = table + "|" + partition
      RedisClusterUtil.hget(tablePartition, key)
    })

    hiveContext.udf.register("c_distinct_set_size", (table: String, partition: String, dimVal: String) => {
      var tablePartition = table + "|" + partition + "|" + dimVal
      RedisClusterUtil.scard(tablePartition)
    })

    hiveContext.udf.register("c_distinct_pf_size", (table: String, partition: String, dimVal: String) => {
      var tablePartition = table + "|" + partition + "|" + dimVal
      RedisClusterUtil.pfcount(tablePartition)
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
  }

}