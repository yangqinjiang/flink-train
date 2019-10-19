package com.imooc.flink.scala.course04

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object DataSetTransformationApp {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //mapFunction(env)
    //filterFunction(env)
    mapPartitionFunction(env)
  }
  //mapPartition
  def mapPartitionFunction(env: ExecutionEnvironment):Unit = {
    //隐式转换
    import org.apache.flink.api.scala._
  val students = new ListBuffer[String]
    for(i<-1 to 100){
      students.append("student: "+ i)
    }
    val data = env.fromCollection(students)
    //使用map 操作数据库,会产生很多数据库链接,与数据源的元素对应
    data.map(x => {
      //每一个元素要存储到数据库,肯定需要先获取一个connection
      val connection = DBUtils.getConection()
      println("------map connection: "+ connection + " -------")
      //TODO:保存到数据库,省略
      DBUtils.returnConnection(connection)
      x
    }).print()

    //使用mapPartition 操作数据库,数据库链接的产生
    data.mapPartition(x => {
      //每一个元素要存储到数据库,肯定需要先获取一个connection
      val connection = DBUtils.getConection()
      println("------mapPartition connection: "+ connection + " -------")
      //TODO:保存到数据库,省略
      DBUtils.returnConnection(connection)
      x
    })
      .setParallelism(2)//控制mapPartition算子里有多少个Db connection
      .print()
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
