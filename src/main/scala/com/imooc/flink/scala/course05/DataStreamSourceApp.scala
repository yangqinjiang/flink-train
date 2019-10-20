package com.imooc.flink.scala.course05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//隐式转换
import org.apache.flink.api.scala._
/**
 * DataStream API编程
 * 测试socket : nc -L -p 9999
 */
object DataStreamSourceApp {


  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
//    socketFunction(env)
//    nonParallelSourceFunction(env)
//    ParallelSourceFunction(env)
    RichParallelSourceFunction(env)
    env.execute("DataStreamSourceApp")
  }
  //支持并行的数据源
  def RichParallelSourceFunction(env: StreamExecutionEnvironment) = {

    val data = env.addSource(new CustomRichParallelSourceFunction())
      .setParallelism(1) // 并行度 > 1 ,支持并行的数据源
    data.print()

  }  //支持并行的数据源
  def ParallelSourceFunction(env: StreamExecutionEnvironment) = {

    val data = env.addSource(new CustomParallelSourceFunction())
      .setParallelism(3) // 并行度 > 1 ,支持并行的数据源
    data.print()

  }
  //不能并行的数据源
  def nonParallelSourceFunction(env: StreamExecutionEnvironment) = {

    val data = env.addSource(new CustomNonParallelSourceFunction())
        .setParallelism(1) // 并行度 > 1 ,数据源输出的不正常
    data.print()

  }

  //侦听socket : localhost:9999
  def socketFunction(env:StreamExecutionEnvironment):Unit = {
    val data = env.socketTextStream("localhost",9999)
    data.print().setParallelism(1)//输出程序的并行度
  }
}
