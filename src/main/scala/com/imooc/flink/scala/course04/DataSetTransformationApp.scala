package com.imooc.flink.scala.course04

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetTransformationApp {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //mapFunction(env)
    filterFunction(env)
  }
  def filterFunction(env: ExecutionEnvironment): Unit = {
    //隐式转换
    import org.apache.flink.api.scala._
    val list = 1 to 10  //List(1,2,3,4,5,6,7,8,9,10)
    val data = env.fromCollection(list)
    //map -> filter -> map 链式调用
    data.map(_ + 1).filter(_>5).print()
    data.map(_ + 1).filter(_>5).map(_ + 1).print()
  }

  //map
  def mapFunction (env:ExecutionEnvironment) :Unit = {
    //隐式转换
    import org.apache.flink.api.scala._
    val list = 1 to 10  //List(1,2,3,4,5,6,7,8,9,10)
    val data = env.fromCollection(list)
    //方式一
    data.map((x:Int)=>x+1).print()
    //方式二
    data.map((x)=>x+1).print()
    //方式三
    data.map(x=>x+1).print()
    //方式四
    data.map(_ + 1).print()
  }
}
