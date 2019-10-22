package com.imooc.flink.scala.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//隐式转换
import org.apache.flink.api.scala._

/**
 * flink对接kafka,作为source使用
 */
object KafkaConnectorConsumerApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val topic = "pktest"//kafka topic
    val prop = new Properties();
    //如果使用hadoop000 ,必须配置host文件
    prop.setProperty("bootstrap.servers","hadoop000:9092")
    prop.setProperty("group.id","test")
    val data = env.addSource(new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),prop))
    data.print()
    env.execute("KafkaConnectorConsumerApp")
  }
}
