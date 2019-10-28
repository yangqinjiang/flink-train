package com.imooc.flink.scala.project

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.imooc.flink.java.project.LogSourceFunction
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.mutable
//隐式转换
import org.apache.flink.api.scala._

/**
 * 消费 kafka 的日志,并且用户id和域名的映射关系
 */
object LogAnalysis02 {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //数据来源
    val consumer = new SourceFunctionFactory().create(false)
    val data = env.addSource(consumer).setParallelism(2)//source的并行度

    //data.print().setParallelism(1)


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
    //读取 用户id和域名的映射关系
    //读取mysql并行度,应该与全局的保持一致
    val mysqlData = env.addSource(new PKMySQLSource)//.setParallelism(1)
//    mysqlData.print().setParallelism(1)
    //log与mysql表user_domain_config关联
    val connectData = logObject.connect(mysqlData)
      //IN1 ,IN2 ,OUT
      .flatMap(new CoFlatMapFunction[(Long,String,Long),mutable.HashMap[String,String],(Long,String,Long,Int)] {
        var userDomainMap = new mutable.HashMap[String,String]()
        //读取log的数据
        override def flatMap1(value: (Long, String, Long), out: Collector[(Long,String,Long,Int)]): Unit = {
          val domain = value._2
          val useridStr = userDomainMap.getOrElse(domain,"*")//默认值为空
          if( "*" .equals(useridStr)){
            println(s"ERROR: ${domain} 没有找到关联的userid")
          }
          var userid = 0
          try {
            userid = useridStr.toInt
          }catch {
            case e: Exception =>  {
              println(s"ERROR: ${useridStr} 转换成int失败")
            }
          }
          println("flatMap1 = " + value._1 + "\t" + value._2 + "\t" + value._3 + "\t" + userid)
          out.collect((value._1,value._2,value._3,userid))
        }
        //读取MySQL的数据
        override def flatMap2(value: mutable.HashMap[String, String], out: Collector[(Long,String,Long,Int)]): Unit = {
          println("flatMap2 只执行一次,map="+ value.toString())
          userDomainMap = value // 赋值给userDomainMap,给flatMap1 函数使用?
        }
      })
//    connectData.print()
//    env.execute("project - LogAnalysis02")
//    return

    //使用水印, 处理数据无序的问题
    val resultData = connectData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long,String,Long,Int)] {
      val maxOutOfOrderness = 10000L
      var currentMaxTimestamp :Long = _
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long,Int), previousElementTimestamp: Long): Long = {
      val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp,currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1)//此处是按照域名进行keyBy的
        .window(TumblingEventTimeWindows.of(Time.seconds(60)))// 60s 统计一次?
        .apply(new WindowFunction[(Long,String,Long,Int),(String,String,Long,Int),Tuple,TimeWindow] {
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long,Int)], out: Collector[(String, String, Long,Int)]): Unit = {
            val domain = key.getField(0).toString //域名
            var sum = 0L
            var mins = 0L;//这一分钟
            var userid = 0;//这一分钟
            val iterator = input.iterator
            while (iterator.hasNext){
              val next = iterator.next()
              sum += next._3 //traffic 求和
              mins = next._1 //读取到这一分钟的完整时间 2019-09-09 20:20:20
              userid = next._4 //用户ID
            }

            /**
             * 第一个参数: 这一分钟的时间 2019-09-09 20:20
             * 第二个参数: 域名
             * 第三个参数: traffic的和
             */
            val currentMins = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(mins)
            println("keyBy , window ,apply to es: "+ userid)
            out.collect((currentMins.toString,domain,sum,userid))
          }
        })

    resultData.print().setParallelism(1)
    //保存到ES
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200,"http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String,String,Long,Int)](
      httpHosts,new ElasticsearchSinkFunction[(String, String, Long,Int)] {
        def createIndexRequest(t: (String, String, Long,Int)):IndexRequest = {
          val json = new java.util.HashMap[String,Any]()
          json.put("time",t._1)
          json.put("domain",t._2)
          json.put("traffics",t._3)
          json.put("userid",t._4)
          val id = t._1 +"-"+t._2  //key
          println("write to es: "+ id)
          Requests.indexRequest().index("cdn2").`type`("traffic").id(id).source(json)
        }
        override def process(t: (String, String, Long,Int), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )
    esSinkBuilder.setBulkFlushMaxActions(1)

    resultData.addSink(esSinkBuilder.build())

    env.execute("project - LogAnalysis02")

  }

}
