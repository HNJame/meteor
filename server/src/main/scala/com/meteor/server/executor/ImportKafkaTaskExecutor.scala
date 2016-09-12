package com.meteor.server.executor

import org.apache.spark.rdd.RDD

import com.meteor.model.view.importqueue.ImportKafkaTask
import com.meteor.server.context.ExecutorContext
import com.meteor.server.executor.instance.InstanceTaskExecutor
import org.apache.spark.storage.StorageLevel

class ImportKafkaTaskExecutor extends AbstractTaskExecutor {

  override def exec(instanceTaskExecutor: InstanceTaskExecutor, paramMap: Map[String, Any]): Unit = {
    val task = instanceTaskExecutor.instanceTask.getTask.asInstanceOf[ImportKafkaTask]
    val rdd = paramMap.get("rdd").get.asInstanceOf[RDD[String]]
    rdd.persist(StorageLevel.MEMORY_ONLY)
    
    val one = rdd.first()
    val oneRDD = ExecutorContext.hiveContext.sparkContext.parallelize(Seq(one), 1)
    val oneSchema = ExecutorContext.hiveContext.read.json(oneRDD).schema

    val reader = ExecutorContext.hiveContext.read
    reader.schema(oneSchema)
    reader.json(rdd).registerTempTable(task.getRegTable)
    ExecutorContext.hiveContext.sql(s"CACHE TABLE ${task.getRegTable}")
    rdd.unpersist(false)
  }

}