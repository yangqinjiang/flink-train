package com.imooc.flink.scala.project

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
 * 读取 user_domain_config表,
 * 结构如下
 * CREATE TABLE user_domain_config(
 * id int unsigned auto_increment,
 * user_id varchar(40) not null,
 * domain varchar(40) not null,
 * primary key (id)
 * )
 *
 * 返回的结果是:一个Map,里面包含(domain,user_id)
 *
 */
class PKMySQLSource extends RichParallelSourceFunction[mutable.HashMap[String,String]]{

  var connection:Connection = null
  var ps:PreparedStatement = null

  //open,建立数据库的链接
  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/imooc_flink"
    val user = "root"
    val password = ""

    Class.forName(driver)
    connection = DriverManager.getConnection(url,user,password)
    val sql = "SELECT user_id,domain from user_domain_config";
    ps = connection.prepareStatement(sql)
  }

  //释放资源
  override def close(): Unit = {
    if (ps != null){
      ps.close()
    }
    if (connection != null){
      connection.close()
    }
  }

  //此处是代码的关键,要从MySQL表中把数据读取出来,转换成Map进行数据的封装
  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {
    println("~~~~~~~~~~~~~run~~~~~~~~~~")
    val data = new mutable.HashMap[String,String]()
    val resultSet:ResultSet = ps.executeQuery()
    while (resultSet.next()){
      data.put(resultSet.getString("domain"),resultSet.getString("user_id"))
    }


    ctx.collect(data)
  }

  override def cancel(): Unit = ???
}
