package com.junyao.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.junyao.bean.StartUpLog
import com.junyao.constants.GmallConstants
import com.junyao.handler.DauHandler
import com.junyao.utils.MyKafkaUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author wjy
 * @create 2022-06-21 15:25
 */
object DauApp {
    def main(args: Array[String]): Unit = {
        //创建sparkConf
        val sparkConf = new SparkConf().setAppName("DauAPP").setMaster("local[*]")

        //创建StreamingContext
        val ssc = new StreamingContext(sparkConf,Seconds(3))

        //通过kafka工具类消费启动日志数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

        //4.将消费到的json字符串转为样例类，为了方便操作，并补全logdate&logHour这两个字段
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
            partition.map(recode => {
                //将json字符串转为样例类
                val startUpLog: StartUpLog = JSON.parseObject(recode.value(), classOf[StartUpLog])

                //补全两个字段
                val times: String = sdf.format(new Date(startUpLog.ts))
                startUpLog.logDate = times.split(" ")(0)
                startUpLog.logHour = times.split(" ")(1)
                startUpLog
            })
        })
        //缓存数据
        startUpLogDStream.cache()

        //打印原始数据条数
        startUpLogDStream.count().print()

        //5、对相同日期相同mid的数据做批次间去重
        val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)
        //缓存数据
        filterByRedisDStream.cache()

        //打印经过批次间去重后的数据条数
        filterByRedisDStream.count().print()

        //6、再对经过批次间去重的数据做批次内去重（先批次间后批次内->总的处理数据量会更少）
        val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)
        filterByGroupDStream.cache()
        filterByGroupDStream.count().print()

        //7、将经历两次去重后的mid写入Redis,目的是为了方便批次间去重，拿当前批次数据对比上一批次数据（缓存在redis中）
        DauHandler.saveToRedis(filterByGroupDStream)

        //将最终去重后的明细数据写入HBase（具有幂等性）
        filterByGroupDStream.foreachRDD(rdd=>{
            rdd.saveToPhoenix(
                "GMALL2022_DAU",
                Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
                HBaseConfiguration.create,
                Some("hadoop102,hadoop103,hadoop104:2181")
            )
        })

        //打印测试
        //startUpLogDStream.print()

        //启动并阻塞
        ssc.start()
        ssc.awaitTermination()
    }
}