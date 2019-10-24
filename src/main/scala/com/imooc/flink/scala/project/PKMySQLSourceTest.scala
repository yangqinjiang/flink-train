package com.imooc.flink.scala.project

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//隐式转换
import org.apache.flink.api.scala._
object PKMySQLSourceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.addSource(new PKMySQLSource).setParallelism(1)
    data.print()
    env.execute("PKMySQLSourceTest")
  }
}
