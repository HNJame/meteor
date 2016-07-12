package com.meteor.server.executor

import java.util.UUID

import com.meteor.model.view.export.ExportKafkaTask
import com.meteor.server.context.ExecutorContext
import com.meteor.server.context.KafkaProducerSingleton
import com.meteor.server.executor.instance.InstanceTaskExecutor
import com.meteor.server.util.Logging
import com.meteor.server.util.PerformanceUtil

import kafka.producer.KeyedMessage

class ExportKafkaTaskExecutor extends AbstractTaskExecutor with Logging {

  override def exec(instanceTaskExecutor: InstanceTaskExecutor, paramMap: Map[String, Any]): Unit = {
    val task = instanceTaskExecutor.instanceTask.getTask.asInstanceOf[ExportKafkaTask]
    val toBrokers = task.getToBrokers
    val toTopic = task.getToTopic
    val fileId = task.getFileId
    ExecutorContext.hiveContext.sql(task.getFetchSql).toJSON.foreachPartition(p => {
      val msgList = new java.util.ArrayList[KeyedMessage[String, String]]
      var lastRow = ""
      for (r <- p) {
        val msg = new KeyedMessage[String, String](toTopic, UUID.randomUUID().toString, r);
        msgList.add(msg)
        lastRow = r
      }

      val producer = KafkaProducerSingleton.getInstance(toBrokers)
      producer.send(msgList)

      PerformanceUtil.sendData(lastRow, fileId)
    })
  }

}