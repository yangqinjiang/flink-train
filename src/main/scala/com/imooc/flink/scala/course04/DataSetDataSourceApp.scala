package com.imooc.flink.scala.course04

import org.apache.flink.api.scala.ExecutionEnvironment
//隐式转换
import org.apache.flink.api.scala._

object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    fromCollection(env)
  }
  //从本地集合读取数据，并打印
  def fromCollection(env:ExecutionEnvironment):Unit = {

    val data = 1 to 10
    env.fromCollection(data).print()
  }
}
