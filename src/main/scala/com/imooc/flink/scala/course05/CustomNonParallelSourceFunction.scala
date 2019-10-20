package com.imooc.flink.scala.course05

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * 自定义SourceFunction,不能并行
 * 每隔1s,产生一个自增的数字
 */
class CustomNonParallelSourceFunction extends SourceFunction[Long]{
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


}
