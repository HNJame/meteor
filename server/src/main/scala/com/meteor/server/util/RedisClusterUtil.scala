package com.meteor.server.util

import java.util.HashSet
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import org.apache.commons.lang.StringUtils

import com.meteor.server.context.ExecutorContext

import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster

/**
 * Created by chenwu on 2015/5/12 0012.
 */
object RedisClusterUtil extends Logging {

  val hostAndPortSet = new HashSet[HostAndPort]()
  val redisClusterHostPortArr = StringUtils.split(ExecutorContext.redisClusterHostPorts, ",")
  for (hostPort: String <- redisClusterHostPortArr) {
    val hostPortSplit = StringUtils.split(hostPort, ":")
    hostAndPortSet.add(new HostAndPort(hostPortSplit(0), Integer.parseInt(hostPortSplit(1))))
  }

  val jedisCluster = new JedisCluster(hostAndPortSet)

  val expireMap = new java.util.concurrent.ConcurrentHashMap[String, Integer]
  val scheduledExecutor = Executors.newScheduledThreadPool(1)

  def setex(key: String, value: String, expireSeconds: Integer): Unit = {
    jedisCluster.setex(key, expireSeconds, value)
  }

  def setneex(key: String, value: String, expireSeconds: Integer): Boolean = {
    var result: Boolean = false
    var setResult = jedisCluster.setnx(key, value)
    if (setResult == 1) {
      jedisCluster.expire(key, expireSeconds)
      result = true
    }
    result
  }

  def get(key: String): String = {
    jedisCluster.get(key)
  }

  def exists(key: String): Boolean = {
    jedisCluster.exists(key)
  }

  def del(key: String): Long = {
    jedisCluster.del(key)
  }

  def hincrBy(hname: String, hkey: String, hval: Long, expireSeconds: Integer): Long = {
    val result = jedisCluster.hincrBy(hname, hkey, hval)
    expireMap.put(hname, expireSeconds)
    result
  }

  def hget(hname: String, hkey: String): Long = {
    jedisCluster.hget(hname, hkey).toLong
  }

  def max(key: String, value: Long, expireSeconds: Integer): Long = {
    var maxVal = value.toString
    jedisCluster.zadd(key, value, value.toString)
    expireMap.put(key, expireSeconds)
    val result = jedisCluster.zrevrange(key, 0, 0).iterator()
    if (result.hasNext) {
      maxVal = result.next()
    }
    maxVal.toLong
  }

  def min(key: String, value: Long, expireSeconds: Integer): Long = {
    var minVal = value.toString
    jedisCluster.zadd(key, value, value.toString)
    expireMap.put(key, expireSeconds)
    val result = jedisCluster.zrange(key, 0, 0).iterator()
    if (result.hasNext) {
      minVal = result.next()
    }
    minVal.toLong
  }

  scheduledExecutor.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      try {
        val tmpMap = new java.util.HashMap[String, Integer]
        tmpMap.putAll(expireMap)
        val iter = tmpMap.entrySet().iterator()
        while (iter.hasNext()) {
          val entry = iter.next()
          expireMap.remove(entry.getKey)
          jedisCluster.expire(entry.getKey, entry.getValue)
        }
      } catch {
        case e: Exception => logError("设置redis过期时间失败", e)
      }
    }
  }, 5, 5, TimeUnit.MINUTES)
}
