package com.meteor.server.executor

import org.apache.spark.rdd.RDD

import com.meteor.model.view.importqueue.ImportKafkaTask
import com.meteor.server.context.ExecutorContext
import com.meteor.server.executor.instance.InstanceTaskExecutor

class ImportKafkaTaskExecutor extends AbstractTaskExecutor {

  override def exec(instanceTaskExecutor: InstanceTaskExecutor, paramMap: Map[String, Any]): Unit = {
    val task = instanceTaskExecutor.instanceTask.getTask.asInstanceOf[ImportKafkaTask]
    val rdd = paramMap.get("rdd").get.asInstanceOf[RDD[String]]
    ExecutorContext.hiveContext.read.json(rdd).registerTempTable(task.getRegTable)
    ExecutorContext.hiveContext.sql(s"CACHE TABLE ${task.getRegTable}")
  }

}