package com.cy.gmall0715.realtime.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.cy.gmall0715.common.constant.GmallConstant
import com.cy.gmall0715.realtime.bean.OrderInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import util.{MyKafkaUtil, RedisUtil}
import java.util
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.phoenix.spark._
/**
  * @author cy
  * @create 2019-12-30 15:33
  */
object OrderApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)

    val orderInfoDstream: DStream[OrderInfo] = inputStream.map { record =>
      val jsonString: String = record.value()

      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      //电话号码脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
      orderInfo.consignee_tel = telTuple._1 + "********"

      //增加日期
      val datetimeArr = orderInfo.create_time.split(" ")
      orderInfo.create_date=datetimeArr(0)
      orderInfo.create_hour=datetimeArr(1).split(":")(0)

      orderInfo.is_first_order = "yes"
      orderInfo
    }
    //根据redis中的userid判断用户是否是第一次下单，若redis中没有这个用户则是第一次下单
    /*val orderDstream: DStream[OrderInfo] = orderInfoDstream.transform { rdd => {
      //driver端周期执行
      //连接redis
      val jedis: Jedis = RedisUtil.getJedisClient
      val orderUserIdKey: String = "orderUserId"
      val orderSet: util.Set[String] = jedis.smembers(orderUserIdKey)
      jedis.close()
      //广播redis中获取的id
      val orderUserIdBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(orderSet)
      val orderRDD: RDD[OrderInfo] = rdd.map { order => {
        val orderSet: util.Set[String] = orderUserIdBC.value
        //判断是否已存在
        if (!orderSet.contains(order.user_id)) {
          order.is_first_order = "yes"
        } else {
          order.is_first_order = "no"
        }
      }
        order
      }
      orderRDD
    }
    }

    //批次内判断 是否是第一次下单
    val groupbyUserIdDstream: DStream[(String, Iterable[OrderInfo])] = orderDstream.map(order=>(order.user_id,order)).groupByKey()
    val OrderListDstream: DStream[List[OrderInfo]] = groupbyUserIdDstream.map {
      case (userid, order) => {
        val orderList: List[OrderInfo] = order.toList.sortWith((order1, order2) => formatTs(order1.create_time) < formatTs(order2.create_time))
        orderList(0).is_first_order = "yes"
        if (orderList.size > 1) {
          for (i <- 1 to orderList.size - 1) {
            orderList(i).is_first_order = "no"
          }
        }
        orderList
      }
    }

    val resultDstream: DStream[OrderInfo] = OrderListDstream.flatMap(order=>order)

    //把user_id存入到redis中
    resultDstream.foreachRDD(rdd => {
      rdd.foreachPartition { orderInfos => {
        val jedis: Jedis = RedisUtil.getJedisClient
        for (orderInfo <- orderInfos) {
          //保存下过单的用户id
          val orderUserIdKey: String = "orderUserId"
          jedis.sadd(orderUserIdKey, orderInfo.user_id)
        }
        jedis.close()
      }
      }
    })*/

    //保存到hbase
    orderInfoDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0715_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS",
          "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
          "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO",
          "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR","IS_FIRST_ORDER"),
        new Configuration(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    ssc.start()
    ssc.awaitTermination()
  }
  private def formatTs(date:String)={
    val formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val ts: Long = formatter.parse(date).getTime
    ts
  }
}
