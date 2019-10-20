package com.imooc.flink.scala.course06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

object TableSQLAPI {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val filePath = "c:\\flink\\sales.csv"
    import org.apache.flink.api.scala._
    //读取到DataSet
    val csv = env.readCsvFile[SalesLog](filePath,ignoreFirstLine = true)
    csv.print()
    //DataSet -> Table
    val salesTable = tableEnv.fromDataSet(csv)
    //Table -> table
    tableEnv.registerTable("sales",salesTable)
    //sql
    val resultTable = tableEnv.sqlQuery("select customerId ,sum(amountPaid) money from sales group by customerId")
    //table -> dataSet
    tableEnv.toDataSet[Row](resultTable).print()
    /** output:
     * 4,600.0
     * 3,510.0
     * 1,4600.0
     * 2,1005.0
     */

  }
  case class SalesLog(transactionId :String,
    customerId :String,itemId :String,amountPaid :Double)
}
