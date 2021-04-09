package com.juzishuke.flink

import java.lang

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object HelloWorldNc {

  def main(args: Array[String]): Unit = {

    val port = 9999
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val text = env.socketTextStream("localhost", port, '\n')
    val text222 = env.socketTextStream("localhost", port - 1, '\n')


    val left: KeyedStream[(String, String), String] = text.map(x => {
      (x.split(",")(0), x.split(",")(1))
    })
      .assignTimestampsAndWatermarks(new newTs)
      .keyBy(_._1)

    val rt: KeyedStream[(String, String), String] = text222.map(x => {
      (x.split(",")(0), x.split(",")(1))
    })
      .assignTimestampsAndWatermarks(new newTs)
      .keyBy(_._1)
    left.print()

    left.coGroup(rt)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new LeftJoin)
      .print()



    //    val textResult =
    //      text.flatMap(_.split("\\s"))
    //        .map((_, 1))
    //        .keyBy(_._1)
    //        .reduce((a, b) => (a._1, a._2 + b._2))

    //    textResult.print().setParallelism(1)

    env.execute("打印输入数据")
  }

  class LeftJoin extends CoGroupFunction[(String, String), (String, String), String] {
    override def coGroup(first: lang.Iterable[(String, String)], second: lang.Iterable[(String, String)], out: Collector[String]): Unit = {
      val lf: Iterable[(String, String)] = first.asScala
      val rt: Iterable[(String, String)] = second.asScala

      for (elem <- lf) {
        val key: String = elem._1
        var flag = 0
        for (elem2 <- rt) {
          if (elem2._1 == key) {
            flag = 1
            print(elem + ",\t")
            println(elem2)
          }
          if (flag == 0) {
            println(elem + "null")
          }
        }

      }


    }
  }


  class setTsAndWater extends AscendingTimestampExtractor[(String, String)]{
    override def extractAscendingTimestamp(element: (String, String)): Long = {
      element._2.toLong * 1000L
    }
  }

  class newTs extends BoundedOutOfOrdernessTimestampExtractor[(String, String)](Time.seconds(5)){
    override def extractTimestamp(element: (String, String)): Long = {
      element._2.toLong * 1000L
    }
  }

}