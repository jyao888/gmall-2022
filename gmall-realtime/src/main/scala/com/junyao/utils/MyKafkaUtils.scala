package com.junyao.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author wjy
 * @create 2022-06-21 9:05
 */
//消费kafka数据
object MyKafkaUtils {
  //创建配置信息对象
  private val properties: Properties = PropertiesUtil.load("config.properties")

  //用于初始化链接到集群的地址
  val brokerList: String = properties.getProperty("kafka.broker.list")

  //kafka消费者配置
  val kafkaParam = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "wjy2022",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],

  //可以使用这个配置，latest自动重置偏移量为最新的偏移量
  "auto.offset.reset" -> "latest",
  //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
  //如果是false，会需要手动维护kafka偏移量
  "enable.auto.commit" -> (true: java.lang.Boolean)

  )

  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
     dStream
  }

}
