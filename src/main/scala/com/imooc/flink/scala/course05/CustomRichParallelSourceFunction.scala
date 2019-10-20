package com.imooc.flink.scala.course05

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
//隐式转换

/**
 * 自定义SourceFunction,支持并行
 * 每隔1s,产生一个自增的数字
 */
class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long]{
  var count = 1L
  var isRunning = true
  override def cancel(): Unit = {
    isRunning = false
  }
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      ctx.collect(count)
      count += 1
      Thread.sleep(1000) // 睡眠1s,
    }
  }

  //其它父类函数
  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()
}
