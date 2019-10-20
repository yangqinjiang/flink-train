package com.imooc.flink.scala.course04

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * 分布式缓存
 */
object DistributedCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //隐式转换
    import org.apache.flink.api.scala._
    val filePath = "c:\\flink\\hello.txt"
    //1,注册一个本地/HDFS文件
    env.registerCachedFile(filePath,"pk-scala-dc")
    val data = env.fromElements("hadoop","spark","flink","pyspark","storm")

    data.map(new RichMapFunction[String,String] {
      //2,在open方法中,获取到分布式缓存的内容即可
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("pk-scala-dc")
        val lines = FileUtils.readLines(dcFile) //java object
        //打印文件内容
        //此时会出现一个异常,java集合和scala集合不兼容的问题
//        for(ele <- lines){ //scala语法
//          println(ele)
//        }
        println("输出DistributedCache文件内容" + lines)
//        println(lines)
      }
      override def map(value: String): String = {
        value
      }
    }).print()
  }
}
