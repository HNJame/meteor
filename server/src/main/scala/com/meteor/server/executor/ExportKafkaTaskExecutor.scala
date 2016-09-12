package com.meteor.server.executor

import java.io.StringReader
import java.util.UUID

import scala.util.parsing.json.JSONObject

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

import com.meteor.model.view.export.ExportKafkaTask
import com.meteor.server.context.ExecutorContext
import com.meteor.server.context.KafkaProducerSingleton
import com.meteor.server.executor.instance.InstanceTaskExecutor
import com.meteor.server.util.CustomSQLUtil
import com.meteor.server.util.DropTableUtil
import com.meteor.server.util.Logging
import com.meteor.server.util.PerformanceUtil

import kafka.producer.KeyedMessage
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.SubSelect

class ExportKafkaTaskExecutor extends AbstractTaskExecutor with Logging {

  override def exec(instanceTaskExecutor: InstanceTaskExecutor, paramMap: Map[String, Any]): Unit = {
    val task = instanceTaskExecutor.instanceTask.getTask.asInstanceOf[ExportKafkaTask]
    val toBrokers = task.getToBrokers
    val toTopic = task.getToTopic
    val fileId = task.getFileId
    val instanceFlowId = instanceTaskExecutor.instanceTask.getInstanceFlowId
    var fetchSql = task.getFetchSql

    if (StringUtils.startsWith(fetchSql, "csql_group_by_1:")) {
      fetchSql = StringUtils.substring(fetchSql, 16)
      execForeachPartition(fetchSql, toBrokers, toTopic, fileId)
    } else {
      var df: DataFrame = null
      var targetTable = ""
      if (StringUtils.startsWith(fetchSql, "csql_group_by_n:")) {
        fetchSql = StringUtils.substring(fetchSql, 16)
        val sqlArr = StringUtils.split(fetchSql, ";")
        val tablePreStr = s"${fileId}_${instanceFlowId}"
        targetTable = CustomSQLUtil.execSql(tablePreStr, sqlArr(0))
        if (sqlArr.length > 1 && StringUtils.isNotBlank(sqlArr(1))) {
          val finalSql = StringUtils.replace(sqlArr(1), "$targetTable", targetTable)
          df = ExecutorContext.hiveContext.sql(finalSql)
        } else {
          df = ExecutorContext.hiveContext.table(targetTable)
        }
      } else {
        df = ExecutorContext.hiveContext.sql(fetchSql)
      }

      sendKafka(df, toBrokers, toTopic, fileId)
      DropTableUtil.dropTable(targetTable)
    }
  }

  /**
   *
   */
  def sendKafka(df: DataFrame, toBrokers: String, toTopic: String, fileId: Integer): Unit = {
    df.toJSON.foreachPartition(p => {
      val producer = KafkaProducerSingleton.getInstance(toBrokers)
      val msgList = new java.util.ArrayList[KeyedMessage[String, String]]
      var lastRow = ""
      var i = 0
      var partitionKey = UUID.randomUUID().toString

      for (r <- p) {
        i += 1
        val msg = new KeyedMessage[String, String](toTopic, partitionKey, r);
        msgList.add(msg)
        lastRow = r
        if (i == 1000) {
          producer.send(msgList)
          i = 0
          partitionKey = UUID.randomUUID().toString
        }
      }

      if (msgList.size() > 0) {
        producer.send(msgList)
      }

      PerformanceUtil.sendData(lastRow, fileId)
    })
  }

  /**
   *
   */
  def execForeachPartition(sql: String, toBrokers: String, toTopic: String, fileId: Integer): Unit = {
    val stmt = CCJSqlParserUtil.parse(new StringReader(sql))
    val selectBody = stmt.asInstanceOf[Select].getSelectBody().asInstanceOf[PlainSelect]
    val fromTable = selectBody.getFromItem()

    var dataFrame: DataFrame = null
    if (fromTable.isInstanceOf[SubSelect]) {
      dataFrame = ExecutorContext.hiveContext.sql(fromTable.asInstanceOf[SubSelect].getSelectBody.toString())
    } else {
      dataFrame = ExecutorContext.hiveContext.table(fromTable.toString())
    }

    dataFrame.foreachPartition { p =>
      {
        val aggResultMapList = CustomSQLUtil.exec(sql, p)

        val producer = KafkaProducerSingleton.getInstance(toBrokers)
        val msgList = new java.util.ArrayList[KeyedMessage[String, String]]
        var lastRow = ""
        var i = 0
        var partitionKey = UUID.randomUUID().toString

        for (aggResultMap <- aggResultMapList) {
          for (aggRowMap <- aggResultMap) {
            i += 1
            val jsonStr = JSONObject(aggRowMap.toMap).toString()
            val msg = new KeyedMessage[String, String](toTopic, partitionKey, jsonStr);
            msgList.add(msg)
            lastRow = jsonStr
            if (i == 1000) {
              producer.send(msgList)
              i = 0
              partitionKey = UUID.randomUUID().toString
            }
          }
        }

        if (msgList.size() > 0) {
          producer.send(msgList)
        }

        PerformanceUtil.sendData(lastRow, fileId)
      }
    }
  }
}
