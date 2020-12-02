import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object HelloWorld {

  def main(args: Array[String]): Unit = {

    val port = 9999
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val textResult =
      text.flatMap(_.split("\\s"))
        .map((_, 1))
        .keyBy(_._1)
        .reduce((a, b) => (a._1, a._2 + b._2))

    textResult.print().setParallelism(1)

    env.execute("打印输入数据")
  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}