package com.meteor.server.util

import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.exception.ExceptionUtils

import com.meteor.server.context.ExecutorContext
import com.meteor.server.factory.TaskThreadPoolFactory

/**
 * Created by Administrator on 2015/8/19 0019.
 */
object DropTableUtil extends Logging {

  def dropTable(tableName: String): Unit = {
    if (StringUtils.isBlank(tableName)) {
      return ;
    }
    TaskThreadPoolFactory.cachedThreadPool.submit(new Runnable {
      override def run(): Unit = {
        var i = 0
        while (i < 3) {
          try {
            ExecutorContext.hiveContext.dropTempTable(tableName)
            i = 3
          } catch {
            case e: Exception => {
              i += 1
              if (i == 3) {
                logError(ExceptionUtils.getFullStackTrace(e))
              }
            }
          }
        }
      }
    })
  }
}
