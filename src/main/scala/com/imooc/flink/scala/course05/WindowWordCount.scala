package com.imooc.flink.scala.course05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * DataStream API 编程
 */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",9999)
    val counts = text.flatMap(_.toLowerCase().split("\\W+") )
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5)) //time window
      .sum(1)

    counts.print()
    // dataStream ,必须调用 execute函数
    env.execute("Window Stream WordCount")
  }
}
