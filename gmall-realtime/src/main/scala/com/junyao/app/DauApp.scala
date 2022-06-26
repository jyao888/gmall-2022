package com.junyao.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.junyao.constants.GmallConstants
import com.junyao.handler.DauHandler
import com.junyao.utils.MyKafkaUtil
import com.junyao.bean.StartUpLog
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @author wjy
 * @create 2022-06-24 13:55
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val sparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    //创建StreamingContext,该对象是提交SparkStreamingApp的入口,5s是批处理间隔
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //通过Kafka消费工具类 获取STARTUP主题里的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

//    kafkaDStream.foreachRDD(rdd=>{
//      rdd.foreach(recode=>{
//        println(recode.value())
//      })
//    })

    //4.将消费到的json字符串转为样例类，为了方便操作，并补全logdate&logHour这两个字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(recode => {
        val startUpLog: StartUpLog = JSON.parseObject(recode.value(), classOf[StartUpLog])

        //获取指定格式的时间字符串 yyy-MM-dd HH
        val times: String = sdf.format(new Date(startUpLog.ts))

        //补全两个字段 logdate loghour
        startUpLog.logDate = times.split(" ")(0)
        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })
    //缓存优化
    startUpLogDStream.cache()

    //对相同日期相同mid的数据做批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)
    filterByRedisDStream.cache()

    //再对经过批次间去重后的数据做批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)
    filterByGroupDStream.cache()

    //将最终去重后的mid缓存到redis里面 方便批次间去重
    DauHandler.saveToRedis(filterByGroupDStream);

    //将最终去重后的明细数据写入Hbase
    filterByGroupDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
        "GMALL2022_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //启动并阻塞
    ssc.start()
    ssc.awaitTermination()
  }

}
