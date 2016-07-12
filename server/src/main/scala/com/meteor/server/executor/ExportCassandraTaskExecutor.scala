package com.meteor.server.executor

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

import org.apache.commons.lang3.StringUtils

import com.datastax.driver.core.PreparedStatement
import com.meteor.model.view.export.ExportCassandraTask
import com.meteor.server.context.CassandraContextSingleton
import com.meteor.server.context.ExecutorContext
import com.meteor.server.executor.instance.InstanceTaskExecutor
import com.meteor.server.util.LocalCacheUtil
import com.meteor.server.util.Logging
import com.meteor.server.util.RedisClusterUtil

class ExportCassandraTaskExecutor extends AbstractTaskExecutor with Logging {

  override def exec(instanceTaskExecutor: InstanceTaskExecutor, paramMap: Map[String, Any]): Unit = {
    val task = instanceTaskExecutor.instanceTask.getTask.asInstanceOf[ExportCassandraTask]
    ExecutorContext.hiveContext.sql(task.getFetchSql).toJSON.foreachPartition(p => {
      JSON.globalNumberParser = x => {
        if (x.contains(".")) {
          x.toDouble
        } else {
          x.toLong
        }
      }
      val session = CassandraContextSingleton.getSession()
      var insertPS: PreparedStatement = null
      if (task.getIsOverride == 0) {
        insertPS = CassandraContextSingleton.getPreparedStatement(s"INSERT INTO ${task.getToTable} (key, value) VALUES (?, ?) IF NOT EXISTS", task.getToTable, task.getExpireSeconds)
      } else {
        insertPS = CassandraContextSingleton.getPreparedStatement(s"INSERT INTO ${task.getToTable} (key, value) VALUES (?, ?)", task.getToTable, task.getExpireSeconds)
      }
      for (r <- p) {
        val jsonToMap = JSON.parseFull(r).get.asInstanceOf[Map[String, String]]
        var tableKeyData = new ListBuffer[String]()

        if (StringUtils.isNotBlank(task.getPartitionKey)) {
          tableKeyData += jsonToMap.getOrElse(StringUtils.trim(task.getPartitionKey), null)
        }

        val tableKeys = StringUtils.split(task.getTableKeys, ",")
        for (tableKey <- tableKeys) {
          tableKeyData += jsonToMap.getOrElse(StringUtils.trim(tableKey), null)
        }

        val dataId = tableKeyData.mkString("|")
        val dataKey = task.getToTable + "|" + dataId
        
        var insertFlag = true
        if (task.getIsOverride == 0) {
          if (StringUtils.isBlank(LocalCacheUtil.get(dataKey))) {
            LocalCacheUtil.put(dataKey, r)
            if (StringUtils.isBlank(RedisClusterUtil.get(dataKey))) {
              RedisClusterUtil.setex(dataKey, r, 1800)
            } else {
              insertFlag = false
            }
          } else {
            insertFlag = false
          }
        }
        
        if (insertFlag) {
          session.execute(insertPS.bind(dataId, r))
        }
      }
    })
  }

}