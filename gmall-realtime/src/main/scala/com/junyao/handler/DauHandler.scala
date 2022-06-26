package com.junyao.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.junyao.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author wjy
 * @create 2022-06-24 17:07
 *        业务需求中的复杂方法都放到此类下实现
 */
object DauHandler {
  /**
   * 批次内去重
   * @param filterByRedisDStream
   */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //1.将数据转为KV类型，为了方便使用groupBykey，k：logdate+mid  v：startupLog
    val midWithLogDataToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.mapPartitions(partition => {
      partition.map(log => {
        ((log.mid, log.logDate), log)
      })
    })
    val midWithLogDataToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midWithLogDataToLogDStream.groupByKey()
    //将迭代器转为list集合，然后按照时间戳由小到大排序，取第一条
    val midWithLogDataToListLogDStream: DStream[((String, String), List[StartUpLog])] = midWithLogDataToLogIterDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    //将List集合打散成单个value 虽然只有一条数据
    val value: DStream[StartUpLog] = midWithLogDataToListLogDStream.flatMap(_._2)
    value
  }

  /**
   * 批次间去重
   *
   * @param startUpLogDStream
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //在Driver端创建连接 一个批次一次连接
      val jedis = new Jedis("hadoop102", 6379)
      val time: String = sdf.format(new Date(System.currentTimeMillis()))
      val redisKey: String = "Dau" + time
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //将查询出来的set集合广播到Executor端
      val midBc: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //遍历RDD中的每条数据对比去重
      val logs: RDD[StartUpLog] = rdd.filter(log => {
        val bool: Boolean = midBc.value.contains(log.mid)
        !bool
      })
      jedis.close()
      logs
    })
    value
  }

  /**
   * 将mid写入redis: 1、存什么->mid   2、用什么类型存 ->set  3、redisKey怎么设计 —>"Dau"+logDate
   * @param startUpLogDStream
   */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    //foreachRdd是运行在Driver端
    startUpLogDStream.foreachRDD(rdd=>{
      //foreachPartition是运行在Executor端 使用foreachPartition可以减少创建连接的次数
      rdd.foreachPartition(partition=>{
        //创建连接
        val jedis = new Jedis("hadoop102",6379)
        partition.foreach(log=>{
          //将mid写入redis
          //构建redisKey
          val redisKey: String = "Dau" + log.logDate
          jedis.sadd(redisKey,log.mid)
        })
        //关闭连接
        jedis.close()
      })
    })
  }
}
