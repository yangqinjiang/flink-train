package com.imooc.flink.scala.project

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.imooc.flink.java.project.LogSourceFunction
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
//隐式转换
import org.apache.flink.api.scala._

/**
 * 消费 kafka 的日志
 */
object LogAnalysis {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      //kafka
      val topic = "pktest"

      val prop = new Properties();
      //如果使用hadoop000 ,必须配置host文件
      prop.setProperty("bootstrap.servers","hadoop000:9092")
      prop.setProperty("group.id","test")
      //开发阶段,不从kafka读取数据
     // val consumer = new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),prop);

    val consumer = new LogSourceFunction();// 在进程内产生日志,而不是用kafka
    val data = env.addSource(consumer).setParallelism(2)//source的并行度

    data.print().setParallelism(1)


    val logObject = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)
      var time = 0L

      try{
        //转换数据
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      }catch {
        case e: Exception => {
          //TODO:记录错误日志s
          println(s"time parse error: $timeStr",e.getMessage)
        }
      }

      val domain = splits(5)
      val traffic = splits(6).toLong //转换
      (level,time,domain,traffic)
    })
      .filter(_._2 != 0)//生产环境,应该过滤无效的时间time
                        //对于我们的业务来说,我们只需要统计level=E的即可
                        //对于非level=e的数据,本业务不考虑
      .filter( _._1 == "E")
        .map(x=>{
          (x._2,x._3,x._4) // time,domain,traffic , 去掉了 x._1 ,level
        })

    //在生产上进行业务处理的时候,一定要考虑处理的健壮性以及数据的准确性
    //脏数据或者不符合业务规则的数据,是需要全部过滤掉之后
    //再进行相应业务逻辑的处理

    //数据清洗,就是按照我们的业务规则,把原始输入的数据进行一定业务规则的处理
    //使得满足我们的业务需求为准



    //logObject.print().setParallelism(1)

    //使用水印, 处理数据无序的问题
    val resultData = logObject.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long,String,Long)] {
      val maxOutOfOrderness = 10000L
      var currentMaxTimestamp :Long = _
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
      val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp,currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1)//此处是按照域名进行keyBy的
        .window(TumblingEventTimeWindows.of(Time.seconds(60)))// 60s 统计一次?
        .apply(new WindowFunction[(Long,String,Long),(String,String,Long),Tuple,TimeWindow] {
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {
            val domain = key.getField(0).toString //域名
            var sum = 0L
            var mins = 0L;//这一分钟
            val iterator = input.iterator
            while (iterator.hasNext){
              val next = iterator.next()
              sum += next._3 //traffic 求和
              mins = next._1 //读取到这一分钟的完整时间 2019-09-09 20:20:20
            }

            /**
             * 第一个参数: 这一分钟的时间 2019-09-09 20:20
             * 第二个参数: 域名
             * 第三个参数: traffic的和
             */
            val currentMins = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(mins)
            out.collect((currentMins.toString,domain,sum))
          }
        })

//    resultData.print().setParallelism(1)
    //保存到ES
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200,"http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String,String,Long)](
      httpHosts,new ElasticsearchSinkFunction[(String, String, Long)] {
        def createIndexRequest(t: (String, String, Long)):IndexRequest = {
          val json = new java.util.HashMap[String,Any]()
          json.put("time",t._1)
          json.put("domain",t._2)
          json.put("traffics",t._3)
          val id = t._1 +"-"+t._2  //key
          Requests.indexRequest().index("cdn2").`type`("traffic").id(id).source(json)
        }
        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )
    esSinkBuilder.setBulkFlushMaxActions(1)

    resultData.addSink(esSinkBuilder.build())

    env.execute("project - LogAnalysis")

  }

}
