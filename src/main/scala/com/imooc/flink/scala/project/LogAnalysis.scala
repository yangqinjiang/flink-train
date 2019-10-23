package com.imooc.flink.scala.project

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//隐式转换
import org.apache.flink.api.scala._

/**
 * 消费 kafka 的日志
 */
object LogAnalysis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val topic = "pktest"

    val prop = new Properties();
    //如果使用hadoop000 ,必须配置host文件
    prop.setProperty("bootstrap.servers","hadoop000:9092")
    prop.setProperty("group.id","test")
    val consumer = new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),prop);
    val data = env.addSource(consumer)
    data.print().setParallelism(1)
    env.execute("project - LogAnalysis")
  }

}
