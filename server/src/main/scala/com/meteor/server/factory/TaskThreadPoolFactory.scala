package com.meteor.server.factory

import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

object TaskThreadPoolFactory {
  val cachedThreadPool = Executors.newCachedThreadPool()
  val threadPoolMap = scala.collection.mutable.Map[Integer, ThreadPoolExecutor]()
}