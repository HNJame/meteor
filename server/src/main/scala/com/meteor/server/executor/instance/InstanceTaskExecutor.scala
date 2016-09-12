package com.meteor.server.executor.instance

import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID

import org.apache.commons.lang.SerializationUtils
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.builder.ToStringBuilder
import org.apache.commons.lang.builder.ToStringStyle
import org.apache.commons.lang.exception.ExceptionUtils

import com.meteor.model.enumtype.ExecStatus
import com.meteor.model.instance.InstanceTask
import com.meteor.server.context.ExecutorContext
import com.meteor.server.context.KafkaProducerSingleton
import com.meteor.server.executor.AbstractTaskExecutor
import com.meteor.server.factory.TaskThreadPoolFactory
import com.meteor.server.util.Logging

import kafka.producer.KeyedMessage

class InstanceTaskExecutor extends Serializable with Logging {

  val unFinishedPreSet = scala.collection.mutable.Set[Integer]()
  val unFinishedPostSet = scala.collection.mutable.Set[Integer]()
  var table: String = null
  val instanceTask = new InstanceTask()
  var instanceFlowExecutor: InstanceFlowExecutor = _

  def exec(paramMap: Map[String, Any]): Unit = synchronized {
    if (!StringUtils.equals(instanceTask.getStatus, ExecStatus.Init.name())) {
      return
    }
    val task = instanceTask.getTask
    instanceTask.setReadyTime(new Date)
    val instanceTaskExecutor = this
    val threadPool = TaskThreadPoolFactory.threadPoolMap.get(task.getFileId).get
    val thread = threadPool.submit(new Runnable() {
      override def run(): Unit = {
        instanceTask.setStartTime(new Date)
        instanceTask.setPoolQueueSize(threadPool.getQueue.size)
        instanceTask.setPoolActiveCount(threadPool.getActiveCount)
        if (task.getBeginSleepTime > 0) Thread.sleep(task.getBeginSleepTime)
        instanceTask.setStatus("Running")
        val executor = Class.forName(task.getProgramClass).newInstance().asInstanceOf[AbstractTaskExecutor]
        var retriedTimes = -1
        while (retriedTimes < task.getMaxRetryTimes) {
          try {
            ExecutorContext.streamContext.sparkContext.setLocalProperty("spark.scheduler.pool", task.getPriority.toString())
            executor.exec(instanceTaskExecutor, paramMap)
            instanceTask.setStatus(ExecStatus.Success.name())
            retriedTimes = task.getMaxRetryTimes
          } catch {
            case e: Exception => {
              retriedTimes += 1
              val errorMsg = ExceptionUtils.getFullStackTrace(e)
              logError(s"${retriedTimes} -> ${instanceTask.getInstanceFlowId}, ${task.getFileId}, ${task.getFileName}, \n$errorMsg")
              instanceTask.setRetriedTimes(retriedTimes)
              if (retriedTimes == task.getMaxRetryTimes) instanceTask.setStatus(ExecStatus.Fail.name())
              instanceTask.setLog(instanceTask.getLog + errorMsg + "===============\n\n")
              if (task.getRetryInterval > 0) Thread.sleep(task.getRetryInterval)
            }
          }
        }
        instanceTask.setEndTime(new Date)
        val df = new SimpleDateFormat("HH:mm:ss")
        logInfo(f"\n\n===${instanceFlowExecutor.instanceFlow.getSourceTaskId}%5d  ${task.getFileId}%5d  ${df.format(instanceTask.getStartTime)}  ${df.format(instanceTask.getEndTime)}  ${instanceTask.getEndTime.getTime - instanceTask.getStartTime.getTime}%5d  ${instanceTask.getPoolActiveCount}%5d  ${instanceTask.getPoolQueueSize}%5d  ${instanceTask.getInstanceFlowId}%32s  ${task.getFileName}%-65s")
      }
    })
    thread.get
    if (task.getFinishSleepTime > 0) Thread.sleep(task.getFinishSleepTime)

    instanceFlowExecutor.doNext(task.getFileId)
    instanceFlowExecutor.doClean(task.getFileId)
    instanceFlowExecutor.tryFinish()
    sendStatusToKafka()
  }

  def sendStatusToKafka(): Unit = {
    TaskThreadPoolFactory.cachedThreadPool.submit(new Runnable() {
      override def run(): Unit = {
        val producer = KafkaProducerSingleton.getInstanceByte(ExecutorContext.kafkaClusterHostPorts);
        val message = new KeyedMessage[String, Array[Byte]](ExecutorContext.instanceTaskTopic, UUID.randomUUID().toString, SerializationUtils.serialize(instanceTask));
        producer.send(message)
      }
    })
  }

  override def toString(): String = {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }
}