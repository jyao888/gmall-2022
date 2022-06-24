package com.junyao.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.junyao.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author wjy
 * @create 2022-06-21 21:59
 */
object DauHandler {
  /**
   *
   * @param filterByRedisDStream
   * @return
   */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //1、将数据转换为KV类型。方便使用GroupByKey去重 k：logdate+mid  v：startupLog
    val midWithLogDataToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.mid, log.logDate), log)
    })

    //2.对相同mid 相同日期的数据聚合到一起
    val midWithLogDataToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midWithLogDataToLogDStream.groupByKey()

    //3.将迭代器转换成list集合，再按时间戳由小到大排序 取第一条
    val midWithLogDataToListLogDStream: DStream[((String, String), List[StartUpLog])] = midWithLogDataToIterLogDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //使用flatMap算子打散list集合中的数据
    val value: DStream[StartUpLog] = midWithLogDataToListLogDStream.flatMap(_._2)
    value
  }


  /**
   * 批次间去重
   *
   * @param startUpLogDStream
   * @param sc
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
//    //使用filter算子将当前mid与redis中已经保存的mid做比较，如果有重复的数据 则过滤掉当前重复数据
//    val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
//      //1.创建Redis连接
//      val jedis = new Jedis("hadoop102", 6379)
//      //2.查询redis中mid 是否与当前的mid重复
//      val redisKey: String = "Dau" + log.logDate
//      //使用sismember方法判断一个值是否在redisSet类型中，在的话返回true 否则返回false
//      val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)
//
//      //关闭连接
//      jedis.close()
//      //在的话取反，为false则过滤掉这条数据
//      !boolean
//    })
//    value
//
//

//    //优化①：在每个分区下创建连接
//    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
//      //1、创建Jedis连接 此时能够减少连接数
//      val jedis = new Jedis("hadoop102", 6379)
//      val logs: Iterator[StartUpLog] = partition.filter(log => {
//        //2.查询redis中mid是否与当前的mid重复
//        val redisKey: String = "Dau" + log.logDate
//        val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)
//        !boolean
//      })
//      jedis.close()
//      logs
//    })
//    value

    //优化②：使用广播变量 在Driver端创建连接 即在每个批次下获取连接以此减少连接个数 把redis里的数据取出来
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.在每个批次下创建redis连接，此时连接是在Derive端创建的
      val jedis = new Jedis("hadoop102", 6379)

      //2.在Driver端查询redis中保存的数据
      //因为在Drive端创建连接是拿不到Executor中的数据的 除非你连接创建在Executor端 所以只能拿当前系统的时间
      //这里设置的是3s一个批次 所以零点漂移问题有可能导致重复数据 但是对于日活这个需求来说可以忽略
      val time: String = sdf.format(new Date(System.currentTimeMillis()))
      val redisKey: String = "Dau" + time
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //3.将查询出来的set集合广播到Executor端
      val midsBc: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4.遍历每个rdd中的每条数据去做去重
      val logs: RDD[StartUpLog] = rdd.filter(log => {
        val bool: Boolean = midsBc.value.contains(log.mid)
        !bool
      })
      logs
    })
    value
  }
  /*
  *     第一种方案：是一条数据创建一个redis连接  虽然最后设置关闭连接了 但是可能还没有来的及关 下一条就又创建了 故产生大量连接 实属浪费资源
  *     第二种方案：是一个分区(topic有几个分区 经过算子处理后就会有几个分区)创建一个redis连接  相较还行
  *     第二种方案：是一个RDD(一个批次)创建一个redis连接  在Driver端创建连接   需要使用到广播变量
  *               需要考虑两个问题，Driver可能会发生OOM 零点漂移问题  一般都可以忽略
  *
  * */



  /**
   * 将最终去重后的mid写入Redis，为了方便批次间去重
   *
   * @param startUpLogDStream
   * @return
   */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    //foreachRDD是运行在Driver端的
    startUpLogDStream.foreachRDD(rdd=>{
      //foreachPartition是运行在Executor端的
      //在分区算子下创建连接：1、减少连接数 2、可以避免序列化错误 （连接是不能序列化的 所以放在Executor创建使用没问题）
      rdd.foreachPartition(partition=>{
        val jedis = new Jedis("hadoop102", 6379)
        partition.foreach(log=>{
          val redisKey: String = "Dau"+log.logDate
          jedis.sadd(redisKey,log.mid)
        })
        //在哪里创建连接 就在哪里关闭
        jedis.close()
      })
    })
  }
}
