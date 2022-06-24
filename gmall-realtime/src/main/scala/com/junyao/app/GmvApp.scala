package com.junyao.app

import com.alibaba.fastjson.JSON
import com.junyao.bean.OrderInfo
import com.junyao.constants.GmallConstants
import com.junyao.utils.MyKafkaUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @author wjy
 * @create 2022-06-23 19:26
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("GmvAPP").setMaster("local[*]")

    //2.创建streamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.获取Kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    //4、将消费到的json字符串转为样例类，为了方便操作，create_date&create_hour这两个字段
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(recode => {
        //将JSON字符串转换为样例类
        val orderInfo: OrderInfo = JSON.parseObject(recode.value(), classOf[OrderInfo])
        //补全字段

        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        orderInfo
      })
    })

    //将数据写入Hbase
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2022_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL",
          "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT",
          "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME",
          "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY",
          "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //关闭资源
    ssc.start()
    ssc.awaitTermination()
  }
}
