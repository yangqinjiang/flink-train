package com.imooc.flink.scala.project

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//隐式转换
import org.apache.flink.api.scala._
object PKMySQLSourceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //从MySQL读取数据
    //val data = env.addSource(new PKMySQLSource).setParallelism(1)
    //模拟数据
    val data = env.addSource(new PKUserIdDomainSource).setParallelism(1)
    data.print()
    env.execute("PKMySQLSourceTest")
  }
}
