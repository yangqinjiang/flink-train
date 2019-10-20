package com.imooc.flink.scala.course04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

/**
 * 基于flink编程的计数器开发三步曲
 * 1,定义  val counter = new LongCounter()
 * 2,注册 getRuntimeContext.addAccumulator
 * 3,获取 jobResult.getAccumulatorResult
 */
object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop","spark","flink","pyspark","storm");
    //map与计数器的示例, 不推荐
    data.map(new RichMapFunction[String,Long] {
      var counter = 0l
      override def map(value: String): Long = {
        counter = counter + 1
        println("counter : " + counter)
        counter
      }
    }).setParallelism(3)//使用map算子进行计数时, 并行度 >1,满足不了需求
      .print()


    //推荐:使用内置的计数器
    val info = data.map(new RichMapFunction[String,String] {
      //1,定义计数器
      val counter = new LongCounter()
      override def open(parameters: Configuration): Unit = {
        //2,注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala",counter)
      }
      override def map(value: String): String = {
        counter.add(1) //累计加一
        value
      }
    }).setParallelism(3)//使用map算子和计数器  ,进行计数时, 并行度 >1,满足需求

    //读取job的执行结果,有以下的方式

    val usePrint = true
    if (usePrint){
      //原因是print()方法自动会调用execute()方法，造成错误，所以注释掉env.execute()即可
      info.print() //
      val jobResult = env.getLastJobExecutionResult
      //读取计数器的内容
      val count = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
      println("count : "+count)
    }else{
      //直接写到文件系统,不能用print()
      info.writeAsText("c:\\flink\\sink-scala-count",FileSystem.WriteMode.OVERWRITE)
      //方式二,从env.execute得到jobResult,执行之前,不能调用print()
      val jobResult = env.execute("CounterApp")
      //读取计数器的内容
      val count = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
      println("count : "+count)
    }






  }
}
