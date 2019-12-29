package com.cy.gmall0715.realtime.app

import java.text.SimpleDateFormat

import util.MyEsUtil
import com.alibaba.fastjson.JSON
import com.cy.gmall0715.common.constant.GmallConstant
import com.cy.gmall0715.realtime.bean.Startup
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import util.{MyKafkaUtil, RedisUtil}
import java.util
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.phoenix.spark._

import scala.collection.mutable.ListBuffer
/**
  * daily active user
  * @author cy
  * @create 2019-12-15 23:07
  */
object DauApp {
  // sparkStreaming 配合redis保存临时状态 进行过滤去重
  //  1  消费kafka 的数据
  //  2  json字符串 -> 转换为一个对象 case class
  //  3  利用redis进行过滤
  //  4  把过滤后的新数据进行写入 redis  ( 当日用户访问的清单)
  //  5  再把数据写入到hbase中
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall0715")
    val ssc: StreamingContext = new StreamingContext(sparkconf,Seconds(5))

    //  1  消费kafka 的数据
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

    //     inputDstream.map(_.value()).print()

    //  2  json字符串 -> 转换为一个对象 case class
    val startupDstream: DStream[Startup] = inputDstream.map(_.value()).map {
      startupjson => {
        val startupLog: Startup = JSON.parseObject(startupjson, classOf[Startup])
        //补上logDate、logHour
        val formattor = new SimpleDateFormat("yyyy-MM-dd H")
        val dateTime: String = formattor.format(new Date(startupLog.ts))

        val dateTimeArr: Array[String] = dateTime.split(" ")
        startupLog.logDate = dateTimeArr(0)
        startupLog.logHour = dateTimeArr(1)
        startupLog
      }
    }
    //  3  利用redis进行过滤
    ////第一批数据不去重，第二批才去重
    val filterDStream: DStream[Startup] = startupDstream.transform {
      //需要多次执行，放到transform中
      rdd => {
        val jedis: Jedis = RedisUtil.getJedisClient
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val today: String = formattor.format(new Date())
        val dauKey = "dau" + today //:2019-12-20
        val dauSet: util.Set[String] = jedis.smembers(dauKey)
        jedis.close()
        //利用广播变量，把清单发给executor,executor根据清单进行比对过滤
        val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
        println("过滤前" + rdd.count() + "条")
        val filterRDD: RDD[Startup] = rdd.filter(startup => {
          val dauSet: util.Set[String] = dauBC.value
          !dauSet.contains(startup.mid)
        })
        println("过滤后" + filterRDD.count() + "条")
        filterRDD
      }
    }

    //本批次 自检去重
    //相同的mid保留第一条
    //相同的mid聚合在一起,取第一条
    val groupByMidDstream: DStream[(String, Iterable[Startup])] = filterDStream.map({ startuplog=>(startuplog.mid,startuplog)}).groupByKey()
    val filteredSefDstream: DStream[Startup] = groupByMidDstream.map {
      case (mid, startupIter) => {
        val topOneList: List[Startup] = startupIter.toList.sortWith(
          (left, right) => {
            left.ts < right.ts
          }
        ).take(1)
        topOneList(0)
      }
    }

    //redis 不能执行一次
    /*val filterDstream: DStream[Startup] = startUpDstream.filter(startup => {
      val dauSet: util.Set[String] = dauBC.value
      !dauSet.contains(startup.mid)
    })
    filterDstream*/

    //3 利用redis过滤  反复连接redis
    /* startUpDstream.filter(startup=>{
       //executor与redis连接消耗比较大，减少连接的次数
       //ssc 5s变化一次redis 通过广播变量
       val jedis: Jedis = RedisUtil.getJedisClient
       val dauKey: String = "dau:"+startup.logDate
       val exists: Boolean = jedis.sismember(dauKey,startup.mid)
       ! exists
     })*/

    filteredSefDstream.cache()

    //  4  把过滤后的新数据进行写入 redis  ( 当日用户访问的清单)
    filteredSefDstream.foreachRDD(rdd=>{
      rdd.foreachPartition{
        startupIter=>{
          val jedis: Jedis = RedisUtil.getJedisClient
          //val startupList = new ListBuffer[Startup]
          for (startup <- startupIter) {
            //  保存当日用户的访问清单 string list set hash zset
            // jedis   type :  set    key :   dau:[日期]   value: mid
            val dauKey: String = "dau:" + startup.logDate
            println(startup)
            jedis.sadd(dauKey,startup.mid)
            //两天后过期
            jedis.expire(dauKey,2*24*3600)
            //添加到集合中
            //startupList.append(startup)
          }
          jedis.close()
          //startup -> es
          //MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_DAU,startupList.toList,null)
        }
      }
    })

    //保存到HBase
    filteredSefDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0715_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
