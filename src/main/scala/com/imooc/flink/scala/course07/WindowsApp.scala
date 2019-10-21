package com.imooc.flink.scala.course07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * tumbling windows滚动窗口 的使用
 * nc 的下载地址
 * https://eternallybored.org/misc/netcat/
 * 测试方式： nc -L -p 9999
 */

object WindowsApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val text = env.socketTextStream("localhost",9999)
    text.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("WindowsApp")
  }
}
