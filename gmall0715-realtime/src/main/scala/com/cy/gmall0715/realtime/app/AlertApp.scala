package com.cy.gmall0715.realtime.app

import java.text.SimpleDateFormat

import util.{ MyEsUtil}
import com.alibaba.fastjson.JSON
import com.cy.gmall0715.common.constant.GmallConstant
import com.cy.gmall0715.realtime.bean.{CouponAlertInfo, EventInfo}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.MyKafkaUtil
import java.util
import java.util.Date

import scala.util.control.Breaks

/**
  * @author cy
  * @create 2020-01-04 9:33
  */
object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alert_app")
    val ssc: StreamingContext = new StreamingContext(sparkconf,Seconds(5))

    val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)

    //    1 同一设备  groupbykey (mid)
    //    2 5分钟内  开窗口  window (窗口大小 （5分钟）， 滑动步长(10秒) )
    //    3 三次及以上用不同账号登录并领取优惠劵    业务判断  判断同一个设备的操作行为组 是否满足条件
    //    4 并且在登录到领劵过程中没有浏览商品
    //    5 整理成要保存预警的格式

    //todo 1.转换成EventInfo样例类
    val eventInfoDstream: DStream[EventInfo] = inputStream.map { record => {
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])

      val formattor = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateTime: String = formattor.format(new Date(eventInfo.ts))

      val dateTimeArr: Array[String] = dateTime.split(" ")
      eventInfo.logDate = dateTimeArr(0)
      eventInfo.logHour = dateTimeArr(1)
      eventInfo
    }
    }
    eventInfoDstream.cache()
    //todo 2.开窗口
    val eventInfoWindowDsteam: DStream[EventInfo] = eventInfoDstream.window(Seconds(300),Seconds(5))

    //todo 3.同一设备，分组
    val eventInfoGroupByMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoWindowDsteam.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()

    //todo 4.业务判断  三次及以上用不同账号登录并领取优惠劵
    val alertInfoDstream = eventInfoGroupByMidDstream.map { case (mid, eventInfoIter) => {
      //保存此mid使用过的uid,需要去重,es没有scala接口，需要用java的类
      val uidSet = new util.HashSet[String]()
      //保存所有的eventid
      val eventList = new util.ArrayList[String]()
      //保存商品id
      val itemSet = new util.HashSet[String]()
      //记录是否有点击商品的
      var isClickItem = false
      //是否预警
      var ifAlert = false

      //找出所有领取优惠券是登录的uid
      Breaks.breakable(
        for (eventInfo: EventInfo <- eventInfoIter) {
          //保存所有的eventid
          eventList.add(eventInfo.evid)
          //领取优惠券行为，保存对应的uid和itemid
          if (eventInfo.evid == "coupon") {
            uidSet.add(eventInfo.uid)
            itemSet.add(eventInfo.itemid)
          }
          //判断是否浏览商品，即是否有点击商品行为
          if (eventInfo.evid == "clickItem") {
            isClickItem = true
            //有点击行为，break
            Breaks.break()
          }
        }
      )
      //三个账号且没点击商品 符合预警条件
      if (uidSet.size() >= 3 && isClickItem == false) {
        ifAlert = true
      }
      (ifAlert, CouponAlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }
    }
    //alertInfoDstream.print()
    //过滤出预警的
    val filteredDstream: DStream[(Boolean, CouponAlertInfo)] = alertInfoDstream.filter(_._1)
    //filteredDstream.print()

    //todo 6 同一设备，每分钟只记录一次预警 去重
    // 利用要保存到的数据库的 幂等性进行去重  PUT
    // ES的幂等性 是基于ID    设备+分钟级时间戳作为id
    filteredDstream.foreachRDD(rdd=>
      //以partition为一个批次保存
      rdd.foreachPartition(alertItr=>{
        val sourceList: List[(String, CouponAlertInfo)] = alertItr.map { case (flag, alertInfo) =>
          (alertInfo.mid + "_" + alertInfo.ts / 60 / 1000, alertInfo)
        }.toList
        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_ALERT,sourceList)
      })
    )
    ssc.start()
    ssc.awaitTermination()

  }
}
