package com.imooc.flink.scala.course05

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//隐式转换
import org.apache.flink.api.scala._
/**
 * 数据流与算子
 */
object DataStreamTransformationApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //filterFunction(env)
//    unionFunction(env)
    splitSelectFunction(env)
    env.execute("DataStreamTransformationApp")
  }
  //split, select 的综合使用
  def splitSelectFunction(env:StreamExecutionEnvironment)= {
    //使用自定义的数据源,
    val data1 = env.addSource(new CustomNonParallelSourceFunction())
    //split数据 分解数据
    val splits = data1.split(new OutputSelector[Long]{
      override def select(value: Long): lang.Iterable[String] = {
        //奇偶数
        val output = new util.ArrayList[String]()
        if (value % 2 == 0){
          output.add("even")
        }else{
          output.add("odd")
        }
        output  //返回,可迭代的数据
      }
    });
    //select数据,输出split的odd数据
    splits.select("odd").print().setParallelism(1)
    //select数据,输出split的even数据
    splits.select("even").print().setParallelism(1)
  }

  //union
  def unionFunction(env: StreamExecutionEnvironment) = {
    //使用自定义的数据源,
    val data1 = env.addSource(new CustomNonParallelSourceFunction())
    val data2 = env.addSource(new CustomNonParallelSourceFunction())
    val data3 = env.addSource(new CustomNonParallelSourceFunction())
    data1.union(data2,data3).print().setParallelism(1)
  }
  //filter
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
