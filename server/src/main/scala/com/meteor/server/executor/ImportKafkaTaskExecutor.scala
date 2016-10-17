package com.meteor.server.executor

import scala.collection.mutable.ListBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import com.meteor.model.view.importqueue.ImportKafkaTask
import com.meteor.server.context.ExecutorContext
import com.meteor.server.executor.instance.InstanceTaskExecutor

class ImportKafkaTaskExecutor extends AbstractTaskExecutor {

  override def exec(instanceTaskExecutor: InstanceTaskExecutor, paramMap: Map[String, Any]): Unit = {
    val task = instanceTaskExecutor.instanceTask.getTask.asInstanceOf[ImportKafkaTask]
    val rdd = paramMap.get("rdd").get.asInstanceOf[RDD[String]]
    rdd.persist(StorageLevel.MEMORY_ONLY)

    val one = rdd.first()
    val oneRDD = ExecutorContext.hiveContext.sparkContext.parallelize(Seq(one), 1)
    val oneSchema = ExecutorContext.hiveContext.read.json(oneRDD).schema

    val list = ListBuffer[StructField]()
    for (sf <- oneSchema.iterator) {
      if (sf.dataType.isInstanceOf[StructType]) {
        list += StructField(sf.name, MapType(StringType, StringType, true))
      } else {
        list += sf
      }
    }
    val mapTypeOneSchema = StructType(list.seq)

    val reader = ExecutorContext.hiveContext.read
    reader.schema(mapTypeOneSchema)
    reader.json(rdd).registerTempTable(task.getRegTable)
    ExecutorContext.hiveContext.sql(s"CACHE TABLE ${task.getRegTable}")

    rdd.unpersist(false)
  }

}