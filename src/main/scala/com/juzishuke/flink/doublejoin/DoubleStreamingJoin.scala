package com.juzishuke.flink.doublejoin

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 实现两个流的join
 *
 * @author Guan Peixiang (guanpeixiang@juzishuke.com)
 * @date 2021/03/23
 */
object DoubleStreamingJoin {

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

    /**
     * 产生用户数据流
     */
    val clickStream: KeyedStream[UserClickLog, String] = env
      .fromElements(
        UserClickLog("user_2", "1500", "click", "page_1"), // (900, 1500)
        UserClickLog("user_3", "2000", "click", "page_1") // (1400, 2000)
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userID)

    val browseStream: KeyedStream[UserBrowseLog, String] = env
      .fromElements(
        UserBrowseLog("user_2", "1000", "browse", "product_1", "10"), // (1000, 1600)
        UserBrowseLog("user_2", "1500", "browse", "product_1", "10"), // (1500, 2100)
        UserBrowseLog("user_2", "1501", "browse", "product_1", "10"), // (1501, 2101)
        UserBrowseLog("user_3", "2602", "browse", "product_1", "10") // (1502, 2102)
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userID)

    /**
     * 实现双流join
     */
    browseStream.intervalJoin(clickStream)
      .between(Time.minutes(-10), Time.seconds(10)) //定义上下界为(-10,0)
      .process(new MyIntervalJoin2)
      .print()
    env.execute()
  }

  class MyIntervalJoin2 extends ProcessJoinFunction[UserBrowseLog, UserClickLog, String]{
    override def processElement(in1: UserBrowseLog, in2: UserClickLog,context: ProcessJoinFunction[ UserBrowseLog, UserClickLog,String]#Context, collector: Collector[String]): Unit = {
      collector.collect(in1 + " ===> " + in2)
    }
  }

  /**
   * 处理函数
   */
  class MyIntervalJoin extends ProcessJoinFunction[UserClickLog, UserBrowseLog, String] {
    override def processElement(left: UserClickLog, right: UserBrowseLog, ctx: ProcessJoinFunction[UserClickLog, UserBrowseLog, String]#Context, out: Collector[String]): Unit = {
      out.collect(left + " ==> " + right)
    }
  }

}

