package com.juzishuke.flink.doublejoin

import java.lang

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
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
        UserBrowseLog("user_2", "1616508082", "browse", "product_1", "10"), // 22
        UserBrowseLog("user_4", "1616508085", "browse", "product_1", "10"), // 25
        UserBrowseLog("user_1", "1616508089", "browse", "product_1", "10"), // 29
        UserBrowseLog("user_3", "1616508095", "browse", "product_1", "10") // 35
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userID)

    val clickStream: KeyedStream[UserClickLog, String] = env
      .fromElements(
        UserClickLog("user_2", "1616508079", "click", "page_1"), // 19
        UserClickLog("user_3", "1616508085", "click", "page_1") //25
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userID)

    //    browseStream.join(clickStream)
    //      .where(_.userID)
    //      .equalTo(_.userID)
    //      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
    //      .apply(func _)
    //      .print()

    browseStream.coGroup(clickStream)
      .where(_.userID)
      .equalTo(_.userID)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new LeftJoin)
      .print()


    env.execute()

  }

  def func(a: UserBrowseLog, b: UserClickLog, out: Collector[String]): Unit = {
    out.collect(a + " ----- " + b)
  }
  class LeftJoin extends CoGroupFunction[UserBrowseLog, UserClickLog, String]{

    override def coGroup(first: lang.Iterable[UserBrowseLog], second: lang.Iterable[UserClickLog], out: Collector[String]): Unit = {
      val left: Iterable[UserBrowseLog] = first.asScala
      val right: Iterable[UserClickLog] = second.asScala
      for (elem1 <- left) {
        val key: String = elem1.userID
        var flag = 0
        for (elem2 <- right) {
          if(elem2.userID == key){
            flag = 1
            out.collect(elem1 + " --- " + elem2)
          }
        }
        if(flag == 0){
          out.collect(elem1 + " ---- " + "null")
        }

      }

    }
  }



}