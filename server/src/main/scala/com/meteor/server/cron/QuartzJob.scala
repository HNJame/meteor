package com.meteor.server.cron

import org.quartz.Job
import org.quartz.JobExecutionContext

import com.meteor.model.view.AbstractTaskDepend
import com.meteor.server.executor.AbstractTaskExecutor
import com.meteor.server.executor.instance.InstanceTaskExecutor
import com.meteor.server.factory.DefTaskFactory

class QuartzJob extends Job {

  override def execute(context: JobExecutionContext): Unit = {
    val jobName = context.getJobDetail().getKey().getName();
    val taskId = Integer.parseInt(jobName);
    exec(taskId);
  }

  def exec(taskId: Integer): Unit = {
    val task = DefTaskFactory.defAllValid.getDefAllMap().get(taskId).asInstanceOf[AbstractTaskDepend]
    val executor = Class.forName(task.getProgramClass).newInstance().asInstanceOf[AbstractTaskExecutor]
    val instanceTaskExecutor = new InstanceTaskExecutor()
    instanceTaskExecutor.instanceTask.setTask(task)
    executor.exec(instanceTaskExecutor, null)
  }

}