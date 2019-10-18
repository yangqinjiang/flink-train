package com.imooc.flink.scala.course02

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * 使用Scala开发Flink的实时处理应用程序
 * WC 统计的数据，来源于socket
 * 运行前，先运行 nc -l -p 9999
 */
object StreamingWCScalaApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",9999)
    //隐式转换
    import org.apache.flink.api.scala._
    text.flatMap(_.split(",")) //按 , 分隔
        .map((_,1))//为每个单词赋上次数为 1
        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .sum(1)
        .print()
        .setParallelism(1)//输出的并行度 为 1

    env.execute("StreamingWCScalaApp")
  }
}
