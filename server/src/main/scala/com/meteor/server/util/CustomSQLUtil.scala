package com.meteor.server.util

import java.io.StringReader
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Future

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON
import scala.util.parsing.json.JSONObject

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

import com.meteor.server.context.ExecutorContext
import com.meteor.server.factory.TaskThreadPoolFactory

import net.sf.jsqlparser.expression.AllComparisonExpression
import net.sf.jsqlparser.expression.AnalyticExpression
import net.sf.jsqlparser.expression.AnyComparisonExpression
import net.sf.jsqlparser.expression.CaseExpression
import net.sf.jsqlparser.expression.CastExpression
import net.sf.jsqlparser.expression.DateValue
import net.sf.jsqlparser.expression.DoubleValue
import net.sf.jsqlparser.expression.ExpressionVisitor
import net.sf.jsqlparser.expression.ExtractExpression
import net.sf.jsqlparser.expression.HexValue
import net.sf.jsqlparser.expression.IntervalExpression
import net.sf.jsqlparser.expression.JdbcNamedParameter
import net.sf.jsqlparser.expression.JdbcParameter
import net.sf.jsqlparser.expression.JsonExpression
import net.sf.jsqlparser.expression.KeepExpression
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.MySQLGroupConcat
import net.sf.jsqlparser.expression.NullValue
import net.sf.jsqlparser.expression.NumericBind
import net.sf.jsqlparser.expression.OracleHierarchicalExpression
import net.sf.jsqlparser.expression.OracleHint
import net.sf.jsqlparser.expression.Parenthesis
import net.sf.jsqlparser.expression.RowConstructor
import net.sf.jsqlparser.expression.SignedExpression
import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.expression.TimeValue
import net.sf.jsqlparser.expression.TimestampValue
import net.sf.jsqlparser.expression.UserVariable
import net.sf.jsqlparser.expression.WhenClause
import net.sf.jsqlparser.expression.WithinGroupExpression
import net.sf.jsqlparser.expression.operators.arithmetic.Addition
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor
import net.sf.jsqlparser.expression.operators.arithmetic.Concat
import net.sf.jsqlparser.expression.operators.arithmetic.Division
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.expression.operators.relational.Between
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression
import net.sf.jsqlparser.expression.operators.relational.GreaterThan
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals
import net.sf.jsqlparser.expression.operators.relational.InExpression
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression
import net.sf.jsqlparser.expression.operators.relational.LikeExpression
import net.sf.jsqlparser.expression.operators.relational.Matches
import net.sf.jsqlparser.expression.operators.relational.MinorThan
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.select.AllColumns
import net.sf.jsqlparser.statement.select.AllTableColumns
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.SelectExpressionItem
import net.sf.jsqlparser.statement.select.SelectItem
import net.sf.jsqlparser.statement.select.SelectItemVisitor
import net.sf.jsqlparser.statement.select.SubSelect

/**
 * Created by Administrator on 2015/8/19 0019.
 */
object CustomSQLUtil extends Logging {

  JSON.globalNumberParser = x => {
    if (x.contains(".")) {
      x.toDouble
    } else {
      x.toLong
    }
  }

  abstract class Result {
    def result: Long
  }

  case class CountDistinct(redisKey: String, opElems: scala.collection.mutable.Set[Any], var result: Long = 0L) extends Result
  case class Sum(redisKey: String, key: String, var sum: Long = 0L, var result: Long = 0L) extends Result
  case class Max(redisKey: String, var max: Long = 0L, var result: Long = 0L) extends Result
  case class Min(redisKey: String, var min: Long = 0L, var result: Long = 0L) extends Result

  def execSql(tablePreStr: String, sql: String): String = {
    val stmt = CCJSqlParserUtil.parse(new StringReader(sql))
    val selectBody = stmt.asInstanceOf[Select].getSelectBody().asInstanceOf[PlainSelect]
    val groupByItems = selectBody.getGroupByColumnReferences()
    val fromTable = selectBody.getFromItem()

    var resultTableName = ""
    var fromTableName = ""
    var dropTableFlag = true
    if (fromTable.isInstanceOf[SubSelect]) {
      fromTableName = execSql(tablePreStr, fromTable.asInstanceOf[SubSelect].getSelectBody.toString())
    } else {
      fromTableName = fromTable.toString()
      dropTableFlag = false
    }

    if (groupByItems == null) {
      resultTableName = s"${tablePreStr}_${UUID.randomUUID().toString().replace("-", "")}"
      selectBody.setFromItem(new Table(fromTableName));
      ExecutorContext.hiveContext.sql(s"cache table $resultTableName as ${selectBody.toString()}")
    } else {
      resultTableName = execMapPartitions(tablePreStr, sql, fromTableName)
    }

    if (dropTableFlag) {
      DropTableUtil.dropTable(fromTableName)
    }
    resultTableName
  }

  def execMapPartitions(tablePreStr: String, sql: String, fromTableName: String): String = {
    val dataFrame: DataFrame = ExecutorContext.hiveContext.table(fromTableName)
    val mapPartitionRDD = dataFrame.mapPartitions { p =>
      {
        val result = scala.collection.mutable.ListBuffer[String]()
        val aggResultMapList = CustomSQLUtil.exec(sql, p)
        for (aggResultMap <- aggResultMapList) {
          for (aggRowMap <- aggResultMap) {
            result += JSONObject(aggRowMap.toMap).toString()
          }
        }
        result.toIterator
      }
    }

    val resultTableName = s"${tablePreStr}_${UUID.randomUUID().toString().replace("-", "")}"
    mapPartitionRDD.persist(StorageLevel.MEMORY_ONLY)

    val one = mapPartitionRDD.first()
    val oneRDD = ExecutorContext.hiveContext.sparkContext.parallelize(Seq(one), 1)
    val oneSchema = ExecutorContext.hiveContext.read.json(oneRDD).schema

    val reader = ExecutorContext.hiveContext.read
    reader.schema(oneSchema)
    reader.json(mapPartitionRDD).registerTempTable(resultTableName)

    ExecutorContext.hiveContext.sql(s"CACHE TABLE $resultTableName")
    mapPartitionRDD.unpersist(false)
    resultTableName
  }

  def exec(sql: String, p: Iterator[Row]): ListBuffer[Iterable[scala.collection.mutable.Map[String, Any]]] = {
    val dataList = p.toList
    val stmt = CCJSqlParserUtil.parse(new StringReader(sql))
    val selectBody = stmt.asInstanceOf[Select].getSelectBody().asInstanceOf[PlainSelect]

    val selectItems = selectBody.getSelectItems().toList

    val groupByList = selectBody.getGroupByColumnReferences()
    var nThread = 1L
    val groupByListLastIndex = groupByList.size() - 1
    val lastExp = groupByList.get(groupByListLastIndex)
    if (lastExp.isInstanceOf[LongValue]) {
      nThread = lastExp.asInstanceOf[LongValue].getValue
      groupByList.remove(groupByListLastIndex)
    }
    val groupByItems = groupByList.toArray().map { x => x.toString() }

    val result = ListBuffer[Iterable[scala.collection.mutable.Map[String, Any]]]()
    if (nThread <= 1L || dataList.size <= nThread) {
      result += execSub(dataList, groupByItems, selectItems)
    } else {
      val splitSize = dataList.size / nThread
      var i = 0
      var subElemList = ListBuffer[Row]()
      val futureList = ListBuffer[Future[Iterable[scala.collection.mutable.Map[String, Any]]]]()
      for (r <- dataList) {
        i += 1
        subElemList += r
        if (i == splitSize) {
          futureList += execSubThread(subElemList.toList, groupByItems, selectItems)
          i = 0
          subElemList = ListBuffer[Row]()
        }
      }

      if (i > 0) {
        futureList += execSubThread(subElemList.toList, groupByItems, selectItems)
      }

      for (future <- futureList) {
        result += future.get
      }
    }

    result
  }

  def execSubThread(p: List[Row], groupByItems: Array[String], selectItems: List[SelectItem]): Future[Iterable[scala.collection.mutable.Map[String, Any]]] = {
    TaskThreadPoolFactory.cachedThreadPool.submit(new Callable[Iterable[scala.collection.mutable.Map[String, Any]]]() {
      override def call(): Iterable[scala.collection.mutable.Map[String, Any]] = {
        execSub(p, groupByItems, selectItems)
      }
    })
  }

  def execSub(p: List[Row], groupByItems: Array[String], selectItems: List[SelectItem]): Iterable[scala.collection.mutable.Map[String, Any]] = {
    val aggResultMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Any]]()
    val allFuncOpMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Any]]()
    val colFutureList = ListBuffer[Future[Tuple2[String, scala.collection.mutable.Map[String, Any]]]]()

    for (selectItem <- selectItems) {
      selectItem.accept(new SelectItemVisitor() {
        override def visit(allColumns: AllColumns): Unit = {
          // TODO Auto-generated method stub

        }

        override def visit(allTableColumns: AllTableColumns): Unit = {
          // TODO Auto-generated method stub

        }

        override def visit(selectExpressionItem: SelectExpressionItem): Unit = {
          var alias = ""
          if (selectExpressionItem.getAlias != null) alias = selectExpressionItem.getAlias.getName
          selectExpressionItem.getExpression().accept(new ExpressionVisitor() {

            override def visit(longValue: LongValue): Unit = {
            }

            override def visit(stringValue: StringValue): Unit = {
            }

            override def visit(tableColumn: Column): Unit = {
            }

            override def visit(function: net.sf.jsqlparser.expression.Function): Unit = {
              val funcName = function.getName()
              if (StringUtils.equalsIgnoreCase(funcName, "c_count_distinct")) {
                val future = TaskThreadPoolFactory.cachedThreadPool.submit(new Callable[Tuple2[String, scala.collection.mutable.Map[String, Any]]]() {
                  override def call(): Tuple2[String, scala.collection.mutable.Map[String, Any]] = {
                    val paramList = function.getParameters().getExpressions()
                    val redisPreKey = paramList(0).asInstanceOf[StringValue].getValue
                    val keyCols = paramList(1).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }
                    val valCols = paramList(2).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }
                    val batchSize = paramList(3).asInstanceOf[LongValue].getValue
                    val resultNThreads = paramList(4).asInstanceOf[LongValue].getValue
                    val expireSeconds = paramList(5).asInstanceOf[LongValue].getValue
                    val isAccurate = paramList(6).asInstanceOf[Column].getColumnName.toBoolean
                    val funcOpMap = scala.collection.mutable.Map[String, Any]()

                    for (r <- p) {
                      val keyColValues = keyCols.map { x => r.getAs(x).toString() }.toList.mkString("|")
                      val valColValues = valCols.map { x => r.getAs(x).toString() }.toList.mkString("|")
                      val redisKey = s"$redisPreKey|$keyColValues"
                      val distinctBuffer = funcOpMap.getOrElseUpdate(keyColValues, CountDistinct(redisKey, scala.collection.mutable.Set[Any](), 0L)).asInstanceOf[CountDistinct]
                      if (batchSize == 1) {
                        if (isAccurate) {
                          RedisClusterUtil.jedisCluster.sadd(redisKey, valColValues)
                        } else {
                          RedisClusterUtil.jedisCluster.pfadd(redisKey, valColValues)
                        }
                      } else {
                        distinctBuffer.opElems += valColValues
                        if (distinctBuffer.opElems.size == batchSize) {
                          if (isAccurate) {
                            RedisClusterUtil.saddMulti(redisKey, distinctBuffer.opElems.asInstanceOf[scala.collection.mutable.Set[String]].toArray)
                          } else {
                            RedisClusterUtil.pfadd(redisKey, distinctBuffer.opElems.asInstanceOf[scala.collection.mutable.Set[String]].toArray)
                          }
                          distinctBuffer.opElems.clear()
                        }
                      }
                    }

                    doResultSplit(funcOpMap, resultNThreads, expireSeconds.toInt, isAccurate)
                    (alias, funcOpMap)
                  }
                })
                colFutureList += future

              } else if (StringUtils.equalsIgnoreCase(funcName, "c_sum")) {
                val future = TaskThreadPoolFactory.cachedThreadPool.submit(new Callable[Tuple2[String, scala.collection.mutable.Map[String, Any]]]() {
                  override def call(): Tuple2[String, scala.collection.mutable.Map[String, Any]] = {
                    val paramList = function.getParameters().getExpressions()
                    val redisPreKey = paramList(0).asInstanceOf[StringValue].getValue
                    val partitionCol = paramList(1).asInstanceOf[Column].getColumnName
                    val keyCols = paramList(2).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }
                    val valCol = paramList(3).asInstanceOf[Column].getColumnName
                    val resultNThreads = paramList(4).asInstanceOf[LongValue].getValue
                    val expireSeconds = paramList(5).asInstanceOf[LongValue].getValue
                    val funcOpMap = scala.collection.mutable.Map[String, Any]()

                    for (r <- p) {
                      val partitionColValue = r.getAs(partitionCol).toString()
                      val keyColValues = keyCols.map { x => r.getAs(x).toString() }.toList.mkString("|")
                      val valColValue = r.getAs[Long](valCol)
                      val redisKey = s"$redisPreKey|$partitionColValue"
                      val dataKey = s"$partitionColValue|$keyColValues"
                      val sumBuffer = funcOpMap.getOrElseUpdate(dataKey, Sum(redisKey, keyColValues, 0L, 0L)).asInstanceOf[Sum]
                      sumBuffer.sum = sumBuffer.sum + valColValue
                    }

                    doResultSplit(funcOpMap, resultNThreads, expireSeconds.toInt, true)
                    (alias, funcOpMap)
                  }
                })
                colFutureList += future

              } else if (StringUtils.equalsIgnoreCase(funcName, "c_max")) {
                val future = TaskThreadPoolFactory.cachedThreadPool.submit(new Callable[Tuple2[String, scala.collection.mutable.Map[String, Any]]]() {
                  override def call(): Tuple2[String, scala.collection.mutable.Map[String, Any]] = {
                    val paramList = function.getParameters().getExpressions()
                    val redisPreKey = paramList(0).asInstanceOf[StringValue].getValue
                    val keyCols = paramList(1).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }
                    val valueCol = paramList(2).asInstanceOf[Column].getColumnName
                    val resultNThreads = paramList(3).asInstanceOf[LongValue].getValue
                    val expireSeconds = paramList(4).asInstanceOf[LongValue].getValue
                    val funcOpMap = scala.collection.mutable.Map[String, Any]()

                    for (r <- p) {
                      val keyColValues = keyCols.map { x => r.getAs(x).toString() }.toList.mkString("|")
                      val valueColValue = r.getAs[Long](valueCol)
                      val redisKey = s"$redisPreKey|$keyColValues"

                      val buffer = funcOpMap.getOrElseUpdate(keyColValues, Max(redisKey, 0L, 0L)).asInstanceOf[Max]
                      if (buffer.max < valueColValue) {
                        buffer.max = valueColValue
                      }
                    }

                    doResultSplit(funcOpMap, resultNThreads, expireSeconds.toInt, true)
                    (alias, funcOpMap)
                  }
                })
                colFutureList += future

              } else if (StringUtils.equalsIgnoreCase(funcName, "c_min")) {
                val future = TaskThreadPoolFactory.cachedThreadPool.submit(new Callable[Tuple2[String, scala.collection.mutable.Map[String, Any]]]() {
                  override def call(): Tuple2[String, scala.collection.mutable.Map[String, Any]] = {
                    val paramList = function.getParameters().getExpressions()
                    val redisPreKey = paramList(0).asInstanceOf[StringValue].getValue
                    val keyCols = paramList(1).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }
                    val valueCol = paramList(2).asInstanceOf[Column].getColumnName
                    val resultNThreads = paramList(3).asInstanceOf[LongValue].getValue
                    val expireSeconds = paramList(4).asInstanceOf[LongValue].getValue
                    val funcOpMap = scala.collection.mutable.Map[String, Any]()

                    for (r <- p) {
                      val keyColValues = keyCols.map { x => r.getAs(x).toString() }.toList.mkString("|")
                      val valueColValue = r.getAs[Long](valueCol)
                      val redisKey = s"$redisPreKey|$keyColValues"

                      val buffer = funcOpMap.getOrElseUpdate(keyColValues, Min(redisKey, 0L, 0L)).asInstanceOf[Min]
                      if (buffer.min > valueColValue) {
                        buffer.min = valueColValue
                      }
                    }

                    doResultSplit(funcOpMap, resultNThreads, expireSeconds.toInt, true)
                    (alias, funcOpMap)
                  }
                })
                colFutureList += future
              }
            }

            override def visit(v: NullValue): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: SignedExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: JdbcParameter): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: JdbcNamedParameter): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: DoubleValue): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: HexValue): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: DateValue): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: TimeValue): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: TimestampValue): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Parenthesis): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Addition): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Division): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Multiplication): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Subtraction): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: AndExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: OrExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Between): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: EqualsTo): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: GreaterThan): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: GreaterThanEquals): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: InExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: IsNullExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: LikeExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: MinorThan): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: MinorThanEquals): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: NotEqualsTo): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: SubSelect): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: CaseExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: WhenClause): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: ExistsExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: AllComparisonExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: AnyComparisonExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Concat): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Matches): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: BitwiseAnd): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: BitwiseOr): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: BitwiseXor): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: CastExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: Modulo): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: AnalyticExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: WithinGroupExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: ExtractExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: IntervalExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: OracleHierarchicalExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: RegExpMatchOperator): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: JsonExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: RegExpMySQLOperator): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: UserVariable): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: NumericBind): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: KeepExpression): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: MySQLGroupConcat): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: RowConstructor): Unit = {
              // TODO Auto-generated method stub

            }

            override def visit(v: OracleHint): Unit = {
              // TODO Auto-generated method stub

            }

          })
        }
      })
    }

    for (future <- colFutureList) {
      val result = future.get()
      allFuncOpMap.put(result._1, result._2)
    }

    for (r <- p) {
      val groupByKeyStr = groupByItems.map { x => r.getAs(x).toString() }.toList.mkString("|")
      val aggRowMap = aggResultMap.getOrElseUpdate(groupByKeyStr, scala.collection.mutable.Map[String, Any]())

      for (selectItem <- selectItems) {
        selectItem.accept(new SelectItemVisitor() {
          override def visit(allColumns: AllColumns): Unit = {
            // TODO Auto-generated method stub

          }

          override def visit(allTableColumns: AllTableColumns): Unit = {
            // TODO Auto-generated method stub

          }

          override def visit(selectExpressionItem: SelectExpressionItem): Unit = {
            var alias = ""
            if (selectExpressionItem.getAlias != null) alias = selectExpressionItem.getAlias.getName
            selectExpressionItem.getExpression().accept(new ExpressionVisitor() {

              override def visit(longValue: LongValue): Unit = {
                aggRowMap.getOrElseUpdate(alias, longValue.getValue)
              }

              override def visit(stringValue: StringValue): Unit = {
                aggRowMap.getOrElseUpdate(alias, stringValue.getValue)
              }

              override def visit(tableColumn: Column): Unit = {
                if (StringUtils.isNotBlank(alias)) {
                  aggRowMap.getOrElseUpdate(alias, r.getAs[Any](tableColumn.getColumnName))
                } else {
                  aggRowMap.getOrElseUpdate(tableColumn.getColumnName, r.getAs[Any](tableColumn.getColumnName))
                }
              }

              override def visit(function: net.sf.jsqlparser.expression.Function): Unit = {
                val funcName = function.getName()
                if (StringUtils.equalsIgnoreCase(funcName, "max")) {
                  val paramList = function.getParameters().getExpressions()
                  val value = r.getAs[Long](paramList(0).asInstanceOf[Column].getColumnName)
                  val sourceValue = aggRowMap.getOrElseUpdate(alias, value)
                  if (value - sourceValue.asInstanceOf[Long] > 0) {
                    aggRowMap.put(alias, value)
                  }

                } else if (StringUtils.equalsIgnoreCase(funcName, "c_uuid")) {
                  aggRowMap.getOrElseUpdate(alias, UUID.randomUUID().toString())

                } else {
                  val hasValue = aggRowMap.getOrElse(alias, null)
                  if (hasValue == null) {
                    val curFuncOpMap = allFuncOpMap.getOrElse(alias, null)
                    if (curFuncOpMap == null) {
                      aggRowMap.put(alias, 0L)
                    } else {
                      if (StringUtils.equalsIgnoreCase(funcName, "c_count_distinct")) {
                        val paramList = function.getParameters().getExpressions()
                        val keyCols = paramList(1).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }

                        val keyColValues = keyCols.map { x => r.getAs(x).toString() }.toList.mkString("|")

                        getResult(curFuncOpMap, aggRowMap, alias, keyColValues)

                      } else if (StringUtils.equalsIgnoreCase(funcName, "c_sum")) {
                        val paramList = function.getParameters().getExpressions()
                        val partitionCol = paramList(1).asInstanceOf[Column].getColumnName
                        val keyCols = paramList(2).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }

                        val partitionColValue = r.getAs(partitionCol).toString()
                        val keyColValues = keyCols.map { x => r.getAs(x).toString() }.toList.mkString("|")

                        getResult(curFuncOpMap, aggRowMap, alias, s"$partitionColValue|$keyColValues")

                      } else if (StringUtils.equalsIgnoreCase(funcName, "c_max")) {
                        val paramList = function.getParameters().getExpressions()
                        val keyCols = paramList(1).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }

                        val keyColValues = keyCols.map { x => r.getAs(x).toString() }.toList.mkString("|")

                        getResult(curFuncOpMap, aggRowMap, alias, keyColValues)

                      } else if (StringUtils.equalsIgnoreCase(funcName, "c_min")) {
                        val paramList = function.getParameters().getExpressions()
                        val keyCols = paramList(1).asInstanceOf[net.sf.jsqlparser.expression.Function].getParameters.getExpressions.map { x => x.asInstanceOf[Column].getColumnName }

                        val keyColValues = keyCols.map { x => r.getAs(x).toString() }.toList.mkString("|")

                        getResult(curFuncOpMap, aggRowMap, alias, keyColValues)
                      }
                    }
                  }
                }
              }

              override def visit(v: NullValue): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: SignedExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: JdbcParameter): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: JdbcNamedParameter): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: DoubleValue): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: HexValue): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: DateValue): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: TimeValue): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: TimestampValue): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Parenthesis): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Addition): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Division): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Multiplication): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Subtraction): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: AndExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: OrExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Between): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: EqualsTo): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: GreaterThan): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: GreaterThanEquals): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: InExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: IsNullExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: LikeExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: MinorThan): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: MinorThanEquals): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: NotEqualsTo): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: SubSelect): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: CaseExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: WhenClause): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: ExistsExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: AllComparisonExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: AnyComparisonExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Concat): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Matches): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: BitwiseAnd): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: BitwiseOr): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: BitwiseXor): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: CastExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: Modulo): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: AnalyticExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: WithinGroupExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: ExtractExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: IntervalExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: OracleHierarchicalExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: RegExpMatchOperator): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: JsonExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: RegExpMySQLOperator): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: UserVariable): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: NumericBind): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: KeepExpression): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: MySQLGroupConcat): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: RowConstructor): Unit = {
                // TODO Auto-generated method stub

              }

              override def visit(v: OracleHint): Unit = {
                // TODO Auto-generated method stub

              }

            })
          }
        })
      }
    }
    aggResultMap.values
  }

  def doResult(funcOpMap: scala.collection.mutable.Map[String, Any], expireSeconds: Int, isAccurate: Boolean): Unit = {
    val expireKeyList = ListBuffer[String]()
    for ((_, bufferObj) <- funcOpMap) {
      bufferObj match {
        case buffer: CountDistinct => {
          if (!buffer.opElems.isEmpty) {
            if (isAccurate) {
              RedisClusterUtil.saddMulti(buffer.redisKey, buffer.opElems.asInstanceOf[scala.collection.mutable.Set[String]].toArray)
            } else {
              RedisClusterUtil.pfadd(buffer.redisKey, buffer.opElems.asInstanceOf[scala.collection.mutable.Set[String]].toArray)
            }
            buffer.opElems.clear()
          }
          if (isAccurate) {
            buffer.result = RedisClusterUtil.scard(buffer.redisKey)
          } else {
            buffer.result = RedisClusterUtil.pfcount(buffer.redisKey)
          }
          expireKeyList += buffer.redisKey
        }

        case buffer: Sum => {
          if (buffer.sum != 0) {
            buffer.result = RedisClusterUtil.hincrBy(buffer.redisKey, buffer.key, buffer.sum)
            expireKeyList += buffer.redisKey
          }
        }

        case buffer: Max => {
          buffer.result = RedisClusterUtil.max(buffer.redisKey, buffer.max)
          expireKeyList += buffer.redisKey
        }

        case buffer: Min => {
          buffer.result = RedisClusterUtil.min(buffer.redisKey, buffer.min)
          expireKeyList += buffer.redisKey
        }

        case somethingElse =>
      }
    }
    RedisClusterUtil.expireKeysThread(expireKeyList, expireSeconds)
  }

  def doResultThread(funcOpMap: scala.collection.mutable.Map[String, Any], expireSeconds: Int, isAccurate: Boolean): Future[_] = {
    TaskThreadPoolFactory.cachedThreadPool.submit(new Runnable() {
      override def run(): Unit = {
        doResult(funcOpMap, expireSeconds, isAccurate)
      }
    })
  }

  def doResultSplit(funcOpMap: scala.collection.mutable.Map[String, Any], resultNThreads: Long, expireSeconds: Int, isAccurate: Boolean): Unit = {
    val funcOpMapSize = funcOpMap.size
    if (resultNThreads <= 1L || funcOpMapSize <= resultNThreads) {
      doResult(funcOpMap, expireSeconds, isAccurate)
    } else {
      val splitSize = funcOpMapSize / resultNThreads
      var i = 0
      var subElemList = scala.collection.mutable.Map[String, Any]()
      val futureList = ListBuffer[Future[_]]()
      for (groupEntry <- funcOpMap) {
        subElemList += groupEntry
        i += 1
        if (i == splitSize) {
          futureList += doResultThread(subElemList, expireSeconds.toInt, isAccurate)
          i = 0
          subElemList = scala.collection.mutable.Map[String, Any]()
        }
      }
      if (i > 0) {
        futureList += doResultThread(subElemList, expireSeconds, isAccurate)
      }
      for (future <- futureList) {
        future.get
      }
    }
  }

  def getResult(curFuncOpMap: scala.collection.mutable.Map[String, Any], aggRowMap: scala.collection.mutable.Map[String, Any], alias: String, groupByKeys: String): Unit = {
    val bufferObj = curFuncOpMap.getOrElse(groupByKeys, null)
    if (bufferObj != null) {
      val buffer = bufferObj.asInstanceOf[Result]
      aggRowMap.put(alias, buffer.result)
    } else {
      aggRowMap.put(alias, 0L)
    }
  }
}
