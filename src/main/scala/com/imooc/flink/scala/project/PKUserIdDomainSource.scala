package com.imooc.flink.scala.project

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
 * 这里是模拟返回MySQl数据库的数据, 仅测试使用
 * 返回的结果是:一个Map,里面包含(domain,user_id)
 *
 */
class PKUserIdDomainSource extends RichParallelSourceFunction[mutable.HashMap[String,String]]{

  //此处是代码的关键,要从MySQL表中把数据读取出来,转换成Map进行数据的封装
  //这里是模拟返回MySQl数据库的数据, 仅测试使用
  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {
    println("~~~~~~~~~~~~~run~~~~~~~~~~")
    val data = new mutable.HashMap[String,String]()
    data.put("v1.go2yd.com","8000000")
    data.put("v2.go2yd.com","8000001")
    data.put("v3.go2yd.com","8000000")
    data.put("v4.go2yd.com","8000002")
    data.put("vmi.go2yd.com","8000000")

    ctx.collect(data)
  }

  override def cancel(): Unit = ???
}
