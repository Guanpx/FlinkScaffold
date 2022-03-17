package com.magic.flink.doublejoin



import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.{CoGroupFunction, RichMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.util.Collector

import java.lang
import java.util.Properties
import scala.collection.JavaConverters._

/**
 *
 * order_flow和maxwell_prd_rms进行join，从hbase补全数据，实现双流left join
 * .99的正常数据，两流的时间差都在10s内
 *
 * @author Guan Peixiang (guanpeixiang@juzishuke.com)
 * @date 2021/04/13
 */
object DoubleStreamingJoin {

  val topic = "topic_left"
  val topicSales = "topic_right"

  /**
   * 窗口大小 延迟时间
   */
  val delayTime = 10
  val timeWindow = 60

  val kafkaBroker = "localhost:9092"
  val zkBroker = "localhost:2181"

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", kafkaBroker)
    props.put("zookeeper.connect", zkBroker)
    props.put("group.id", "fk_test")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristiΩc(TimeCharacteristic.EventTime)
    env.getConfig.setGlobalJobParameters(ParameterTool.fromArgs(args))

    val outTopic: String = ParameterTool.fromArgs(args).get("out")

    /**
     * 数据处理为kv形式
     */
    val consumer = new FlinkKafkaConsumer011[JSONObject](topic, new DataSchema(), props)
    val data: KeyedStream[(String, JSONObject), String] = env.addSource(consumer)
      .map(x => {
        val order: String = x.getString("orderNo")
        (order, x)
      })
      .assignTimestampsAndWatermarks(new setTsAndWater)
      .keyBy(_._1)

    val consumerSalesData: FlinkKafkaConsumer011[JSONObject] = new FlinkKafkaConsumer011[JSONObject](topicSales, new DataSchema(), props)
    val dataSalesData: KeyedStream[(String, JSONObject), String] = env.addSource(consumerSalesData)
      .filter(x => {
        x.getString("table").equals("sales_table")
      })
      .map(x => {
        val data: JSONObject = x.getJSONObject("data")
        val order: String = data.getString("order_no")
        (order, data)
      })
      .assignTimestampsAndWatermarks(new setTsAndWaterRight)
      .keyBy(_._1)

    /**
     * 接受处理迟到数据
     */
    val lateTag: OutputTag[(String, JSONObject)] = new OutputTag[(String, JSONObject)]("late-date")
    val lateDataStream: DataStream[(String, JSONObject)] = data.windowAll(TumblingEventTimeWindows.of(Time.seconds(timeWindow)))
      .allowedLateness(Time.seconds(delayTime))
      .sideOutputLateData(lateTag)
      .apply(new MyTags)
    val lateData: DataStream[JSONObject] = lateDataStream.getSideOutput(lateTag).map(_._2)

    /**
     * 进行双流join
     */
    val res: DataStream[JSONObject] = data.coGroup(dataSalesData)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(timeWindow)))
      .apply(new LeftJoin)
      .map(new hbaseRequest)

    /**
     * 结果输出到kafka
     */
    res.addSink(
      new FlinkKafkaProducer011[JSONObject](kafkaBroker, outTopic, new DataSchema()))

    lateData.map(new hbaseRequest).addSink(
      new FlinkKafkaProducer011[JSONObject](kafkaBroker, outTopic, new DataSchema()))

    env.execute("double stream left join")
  }

  /**
   * left join
   */
  class LeftJoin extends CoGroupFunction[(String, JSONObject), (String, JSONObject), JSONObject] {
    override def coGroup(first: lang.Iterable[(String, JSONObject)], second: lang.Iterable[(String, JSONObject)], out: Collector[JSONObject]): Unit = {
      val left: Iterable[(String, JSONObject)] = first.asScala
      val right: Iterable[(String, JSONObject)] = second.asScala

      for (lf <- left) {
        var flag = false
        val key: String = lf._1
        val v: JSONObject = lf._2
        for (rt <- right) {
          if (rt._1 == key) {
            flag = true
            val salesName: JSONObject = rt._2.getJSONObject("sales_name")
            if (salesName != null) {
              v.put("sales_name", salesName)
            }
            println("++++++this is match v:" + key + " " + salesName)
            out.collect(v)
          }
        }
        if (!flag) {
          println("++++++no match this is:" + key)
          out.collect(v)
        }
      }
    }
  }

  /**
   * 设置事件时间 设置数据延迟
   */
  class setTsAndWaterRight extends BoundedOutOfOrdernessTimestampExtractor[(String, JSONObject)](Time.seconds(delayTime)) {
    override def extractTimestamp(element: (String, JSONObject)): Long = {
      // 可以在这里打印水位等时间信息
      element._2.getString("created_time").toLong
    }
  }

  /**
   * 设置事件时间 设置数据延迟
   */
  class setTsAndWater extends BoundedOutOfOrdernessTimestampExtractor[(String, JSONObject)](Time.seconds(delayTime)) {
    override def extractTimestamp(element: (String, JSONObject)): Long = {
      element._2.getString("started_time").toLong
    }
  }


  class MyTags extends AllWindowFunction[(String, JSONObject), (String, JSONObject), TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[(String, JSONObject)], out: Collector[(String, JSONObject)]): Unit = {
      for (elem <- input) {
        out.collect(elem)
      }
    }
  }


  /**
   * 可以从外部获取数据 data
   * 暂时不需要异步I/O
   */
  class hbaseRequest extends RichMapFunction[JSONObject, JSONObject] {

    override def open(parameters: Configuration): Unit = {

      println("this is open func !!!")
    }

    override def close(): Unit = {

    }

    override def map(value: JSONObject): JSONObject = {
      value
    }
  }
}
