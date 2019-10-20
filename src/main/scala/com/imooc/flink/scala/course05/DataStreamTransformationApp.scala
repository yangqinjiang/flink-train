package com.imooc.flink.scala.course05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//隐式转换
import org.apache.flink.api.scala._
/**
 * 数据流与算子
 */
object DataStreamTransformationApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    filterFunction(env)
    env.execute("DataStreamTransformationApp")
  }

  def filterFunction(env: StreamExecutionEnvironment) = {
    //使用自定义的数据源,
    val data = env.addSource(new CustomNonParallelSourceFunction())
    // 数据流向: map -> filter -> print
    data.map(x=>{
      println("received: " + x)
      x
    }).filter( _%2 == 0)//只要偶数
      .print().setParallelism(1)
  }

}
