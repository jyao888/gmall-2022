package com.junyao.app

import com.alibaba.fastjson.JSON
import com.junyao.bean.OrderInfo
import com.junyao.constants.GmallConstants
import com.junyao.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._


/**
 * @author wjy
 * @create 2022-06-24 19:45
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf = new SparkConf().setMaster("local[*]").setAppName("GmvAPP")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.消费kafka数据
    val GmvDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    //4.将JSON数据转换为样例类并补全create_date&create_hour字段
    val orderInfoDStream: DStream[OrderInfo] = GmvDStream.mapPartitions(partition => {
      partition.map(recode => {
        val orderInfo: OrderInfo = JSON.parseObject(recode.value(), classOf[OrderInfo])
        //补全字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        orderInfo
      })
    })

    //5.将数据写入Phoenix
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL220212_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //6.启动并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
