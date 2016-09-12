package com.meteor.server.executor

import org.apache.commons.lang.StringUtils

import com.meteor.model.view.buildmodel.SqlTask
import com.meteor.server.context.ExecutorContext
import com.meteor.server.executor.instance.InstanceTaskExecutor
import com.meteor.server.util.CustomSQLUtil
import com.meteor.server.util.DropTableUtil
import com.meteor.server.util.Logging

class SqlTaskExecutor extends AbstractTaskExecutor with Logging {

  override def exec(instanceTaskExecutor: InstanceTaskExecutor, paramMap: Map[String, Any]): Unit = {
    val task = instanceTaskExecutor.instanceTask.getTask.asInstanceOf[SqlTask]

    if (StringUtils.startsWith(task.getSql, "csql_group_by_n:")) {
      val sql = StringUtils.substring(task.getSql, 16)
      val sqlArr = StringUtils.split(sql, ";")
      val tablePreStr = s"${task.getFileId}_${instanceTaskExecutor.instanceTask.getInstanceFlowId}"
      val targetTable = CustomSQLUtil.execSql(tablePreStr, sqlArr(0))
      if (sqlArr.length > 1 && StringUtils.isNotBlank(sqlArr(1))) {
        val finalSql = StringUtils.replace(sqlArr(1), "$targetTable", targetTable)
        ExecutorContext.hiveContext.sql(finalSql)
      }
      DropTableUtil.dropTable(targetTable)
    } else {
      ExecutorContext.hiveContext.sql(task.getSql)
    }

    if (task.getRepartition != null && task.getRepartition > 0) {
      val tmpTable = s"${instanceTaskExecutor.table}_tmp"
      ExecutorContext.hiveContext.sql(s"CACHE TABLE ${tmpTable} AS SELECT * FROM ${instanceTaskExecutor.table}")
      ExecutorContext.hiveContext.dropTempTable(instanceTaskExecutor.table)
      ExecutorContext.hiveContext.table(tmpTable).repartition(task.getRepartition).registerTempTable(instanceTaskExecutor.table)
      ExecutorContext.hiveContext.sql(s"CACHE TABLE ${instanceTaskExecutor.table}")
      DropTableUtil.dropTable(tmpTable)
    }
  }

}