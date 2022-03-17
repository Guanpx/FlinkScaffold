package com.magic.flink.doublejoin

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

import java.lang
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.KeyedStream
//import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._


/**
 * <p>
 *
 * @author Guan Peixiang (guanpeixiang@juzishuke.com)
 * @date 2021/03/23
 */
object DoubleWindowJoin {

  // 用户点击日志
  case class UserClickLog(userID: String,
                          eventTime: String,
                          eventType: String,
                          pageID: String)

  // 用户浏览日志
  case class UserBrowseLog(userID: String,
                           eventTime: String,
                           eventType: String,
                           productID: String,
                           productPrice: String)


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val browseStream: KeyedStream[UserBrowseLog, String] = env
      .fromElements(
        UserBrowseLog("user_3", "1616515208", "browse", "product_1", "10"), // 08
        UserBrowseLog("user_4", "1616515208", "browse", "product_1", "10"), // 08
        UserBrowseLog("user_1", "1616515208", "browse", "product_1", "10"), // 08
        UserBrowseLog("user_2", "1616515208", "browse", "product_1", "10") // 08
      )
      .assignTimestampsAndWatermarks(new BoundTsUserBrowseLog)
      .keyBy(_.userID)

    val clickStream: KeyedStream[UserClickLog, String] = env
      .fromElements(
        UserClickLog("user_2", "1616515211", "click", "page_1"), // 11
        UserClickLog("user_3", "1616515202", "click", "page_1") // 02
      )
      .assignTimestampsAndWatermarks(new BoundTsUserClickLog)
      .keyBy(_.userID)

    println(clickStream.getKeyType)

    browseStream.coGroup(clickStream)
      .where(_.userID)
      .equalTo(_.userID)
      .window(TumblingEventTimeWindows.of(Time.seconds(4)))
      .apply(new LeftJoin)
    //      .print()

    env.execute()
  }

  def func(a: UserBrowseLog, b: UserClickLog, out: Collector[String]): Unit = {
    out.collect(a + " ----- " + b)
  }

  class LeftJoin extends CoGroupFunction[UserBrowseLog, UserClickLog, String] {

    override def coGroup(first: lang.Iterable[UserBrowseLog], second: lang.Iterable[UserClickLog], out: Collector[String]): Unit = {
      val left: Iterable[UserBrowseLog] = first.asScala
      val right: Iterable[UserClickLog] = second.asScala
      for (elem1 <- left) {
        val key: String = elem1.userID
        var flag = 0
        for (elem2 <- right) {
          println(elem2)
          if (elem2.userID == key) {
            flag = 1
            out.collect(elem1 + " --- " + elem2)
          }
        }
        if (flag == 0) {
          out.collect(elem1 + " ---- " + "null")
        }

      }

    }
  }


  class BoundTsUserClickLog extends BoundedOutOfOrdernessTimestampExtractor[UserClickLog](Time.seconds(5)) {
    override def extractTimestamp(elem: UserClickLog): Long = {
      //      println(super.getCurrentWatermark)
      //      println(super.getMaxOutOfOrdernessInMillis)
      elem.eventTime.toLong * 1000L
    }
  }

  class BoundTsUserBrowseLog extends BoundedOutOfOrdernessTimestampExtractor[UserBrowseLog](Time.seconds(5)) {
    override def extractTimestamp(elem: UserBrowseLog): Long = {
      //      println(super.getCurrentWatermark)
      //      println(super.getMaxOutOfOrdernessInMillis)
      elem.eventTime.toLong * 1000L
    }
  }

  class setTsAndWater extends AscendingTimestampExtractor[UserClickLog] {
    override def extractAscendingTimestamp(element: UserClickLog): Long = {
      element.eventTime.toLong * 1000L
    }
  }

}