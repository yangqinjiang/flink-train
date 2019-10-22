package com.imooc.flink.scala.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
//隐式转换
import org.apache.flink.api.scala._

/**
 * flink对接kafka,作为Sink使用
 */
object KafkaConnectorProducerApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从socket接收数据,通过flink,将数据写到kafka
    val data = env.socketTextStream("localhost",9999)

    val topic = "pktest"//kafka topic
    val prop = new Properties();
    //如果使用hadoop000 ,必须配置host文件
    prop.setProperty("bootstrap.servers","hadoop000:9092")
    val sink = new FlinkKafkaProducer[String](topic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      prop)
//    val sink = new FlinkKafkaProducer[String](topic,
//      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
//      prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE) //
    data.addSink(sink)

    env.execute("KafkaConnectorProducerApp")
  }
}
