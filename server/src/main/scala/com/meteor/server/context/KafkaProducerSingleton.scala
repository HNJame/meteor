package com.meteor.server.context

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig

/**
 * Created by chenwu on 2015/5/12 0012.
 */
object KafkaProducerSingleton {

  private val producerStrMap = scala.collection.mutable.Map[String, Producer[String, String]]()
  private val producerByteMap = scala.collection.mutable.Map[String, Producer[String, Array[Byte]]]()

  def getInstance(brokers: String): Producer[String, String] = {
    val key = brokers + "Str"
    var result = producerStrMap.getOrElse(key, null)
    if (result == null) result = initInstance(brokers, key)
    result
  }

  def initInstance(brokers: String, key: String): Producer[String, String] = synchronized {
    var producer = producerStrMap.getOrElse(key, null)
    if (producer == null) {
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
      props.put("request.required.acks", "1")
      val config = new ProducerConfig(props)
      producer = new Producer[String, String](config)
      producerStrMap += key -> producer
    }
    producer
  }

  def getInstanceByte(brokers: String): Producer[String, Array[Byte]] = {
    val key = brokers + "Byte"
    var result = producerByteMap.getOrElse(key, null)
    if (result == null) result = initInstanceByte(brokers, key)
    result
  }

  def initInstanceByte(brokers: String, key: String): Producer[String, Array[Byte]] = synchronized {
    var producer = producerByteMap.getOrElse(key, null)
    if (producer == null) {
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")
      props.put("key.serializer.class", "kafka.serializer.StringEncoder")
      props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
      props.put("request.required.acks", "1")
      val config = new ProducerConfig(props)
      producer = new Producer[String, Array[Byte]](config)
      producerByteMap += key -> producer
    }
    producer
  }

}
