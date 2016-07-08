package com.meteor.server.executor

import scala.collection.JavaConversions.asScalaSet
import com.meteor.model.view.AbstractTaskDepend
import com.meteor.model.view.cron.CronTask
import com.meteor.server.executor.instance.InstanceTaskExecutor
import com.meteor.server.factory.DefTaskFactory
import com.meteor.server.factory.TaskThreadPoolFactory
import com.meteor.server.context.KafkaProducerSingleton
import kafka.producer.KeyedMessage
import com.meteor.server.context.ExecutorContext
import java.util.UUID
import org.apache.commons.lang.SerializationUtils
import com.meteor.model.instance.InstanceFlow
import com.meteor.model.enumtype.ExecStatus

class CronTaskExecutor extends AbstractTaskExecutor {

  override def exec(instanceTaskExecutor: InstanceTaskExecutor, paramMap: Map[String, Any]): Unit = {
    val task = instanceTaskExecutor.instanceTask.getTask.asInstanceOf[CronTask]

    val instanceFlow = new InstanceFlow()
    val uuid = UUID.randomUUID().toString
    instanceFlow.setInstanceFlowId(uuid)
    instanceFlow.setSourceTaskId(task.getFileId)
    val date = new java.util.Date
    instanceFlow.setInitTime(date)
    instanceFlow.setStartTime(date)
    instanceFlow.setEndTime(date)
    instanceFlow.setStatus(ExecStatus.Success.name)
    val producer = KafkaProducerSingleton.getInstanceByte(ExecutorContext.kafkaClusterHostPorts);
    val message = new KeyedMessage[String, Array[Byte]](ExecutorContext.instanceFlowTopic, uuid, SerializationUtils.serialize(instanceFlow));
    producer.send(message)

    val paramMap = Map("instanceFlowId" -> uuid)
    for (taskId: Integer <- task.getPostDependSet) {
      TaskThreadPoolFactory.cachedThreadPool.submit(new Runnable() {
        override def run(): Unit = {
          val postTask = DefTaskFactory.defAllValid.getDefAllMap().get(taskId).asInstanceOf[AbstractTaskDepend]
          val executor = Class.forName(postTask.getProgramClass).newInstance().asInstanceOf[AbstractTaskExecutor]
          val instanceTaskExecutor = new InstanceTaskExecutor()
          instanceTaskExecutor.instanceTask.setTask(postTask)
          executor.exec(instanceTaskExecutor, paramMap)
        }
      })
    }
    
  }

}