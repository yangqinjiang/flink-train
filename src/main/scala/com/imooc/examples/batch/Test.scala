package com.imooc.examples.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment

object Test {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    var tableEnv = TableEnvironment.getTableEnvironment(env)
    val filePath = "E:\\sales.csv" //读取windows系统的文件
    //隐式转换
    import org.apache.flink.api.scala._
    val csv = env.readCsvFile[SalesLog](filePath,ignoreFirstLine = true)
    csv.print()
  }

}
case class SalesLog(transactionId:String,
                    customerId:String,
                    itemId:String)
