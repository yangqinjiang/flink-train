package com.imooc.flink.scala.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

/**
 * Sink的基本使用,写到文件夹或者文件
 */
object SinkApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //隐式转换
    import org.apache.flink.api.scala._
    val data = 1.to(10)
    val text = env.fromCollection(data) //数据源
    val filePath = "c:\\flink\\slink-out\\"
    //设置参数, FileSystem.WriteMode.OVERWRITE ,已存在filePath,则覆盖
    //如果 setParallelism 设置并行度 > 1,则slin-out是文件夹, 否则是 文件
    text.writeAsText(filePath,FileSystem.WriteMode.OVERWRITE).setParallelism(2)
    env.execute("SinkApp")
  }
}
