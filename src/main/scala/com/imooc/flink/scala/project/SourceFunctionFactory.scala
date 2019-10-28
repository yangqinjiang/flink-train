package com.imooc.flink.scala.project

import java.util.Properties

import com.imooc.flink.java.project.LogSourceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

class SourceFunctionFactory {
  /**
   *
   * @param viaKafka source 选择,true则是kafka, false  则是自定义类
   * @return
   */
  def create(viaKafka:Boolean):SourceFunction[String] = {
    //定义值为null时,需要声明类型
    var consumer:SourceFunction[String] = null
    //
    if (viaKafka){
      //kafka
      val topic = "pktest"
      val prop = new Properties();
      //如果使用hadoop000 ,必须配置host文件
      prop.setProperty("bootstrap.servers","hadoop000:9092")
      prop.setProperty("group.id","test")
      //开发阶段,不从kafka读取数据
      consumer = new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),prop);
    }else{
      consumer = new LogSourceFunction();// 在进程内产生日志,而不是用kafka
    }
    consumer
  }
}
