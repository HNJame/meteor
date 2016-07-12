package com.meteor.server.util

import java.util.UUID

import scala.util.parsing.json.JSON

import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.DateUtils

import com.meteor.server.context.ExecutorContext
import com.meteor.server.context.KafkaProducerSingleton
import com.meteor.server.factory.TaskThreadPoolFactory

import kafka.producer.KeyedMessage

/**
 * Created by Administrator on 2015/8/19 0019.
 */
object PerformanceUtil extends Serializable with Logging {

  def sendData(jsonRow: String, fileId: Integer): Unit = {
    TaskThreadPoolFactory.cachedThreadPool.submit(new Runnable {
      override def run(): Unit = {
        if (StringUtils.isBlank(jsonRow)) {
          return
        }
        JSON.globalNumberParser = x => {
          if (x.contains(".")) {
            x.toDouble
          } else {
            x.toLong
          }
        }
        val producer = KafkaProducerSingleton.getInstance(ExecutorContext.kafkaClusterHostPorts)
        val jsonToMap = JSON.parseFull(jsonRow).get.asInstanceOf[Map[String, String]]
        val stime = jsonToMap.getOrElse("stime", null)
        if (StringUtils.isBlank(stime)) {
          return
        }
        val spendTime = System.currentTimeMillis() - DateUtils.parseDate(stime, Array("yyyy-MM-dd HH:mm:ss")).getTime
        val msg = new KeyedMessage[String, String](ExecutorContext.performanceTopic, UUID.randomUUID().toString, s"$fileId|$spendTime")
        producer.send(msg)
      }
    })
  }
}
