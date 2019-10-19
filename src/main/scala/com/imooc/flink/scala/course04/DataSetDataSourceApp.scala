package com.imooc.flink.scala.course04

import com.imooc.flink.java.course04.Person
import org.apache.flink.api.scala.ExecutionEnvironment
//隐式转换
import org.apache.flink.api.scala._

/**
 * 数据源
 */
object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //fromCollection(env)
//    readTextFile(env)
//    readTextDir(env)
    csvFile(env)
  }
  //从csv文件读取数据,并打印
  def csvFile(env: ExecutionEnvironment): Unit = {
    /** 文件内容示例:
     * name,age,job
     * Jorge,30,Developer
     * Bob,32,Developer
     */
    val filePath = "c:\\flink\\people.csv"
    //指定数据类型或者tuple元组
    //并且不要解析第一行数据
    //输出三列数据
    env.readCsvFile[(String,Int,String)](filePath,ignoreFirstLine = true).print()
    //输出三列数据,includedFields对应csv文件的顺序号,如0->name,  1->age,  2->job
    /** 文件内容示例:
     * name,age,job
     * Jorge,30,Developer
     * Bob,32,Developer
     */
    env.readCsvFile[(String,Int,String)](filePath,ignoreFirstLine = true,includedFields = Array(0,1,2)).print()
    //输出csv文件的两列数据 name,job , includedFields = 0,2
    env.readCsvFile[(String,String)](filePath,ignoreFirstLine = true,includedFields = Array(0,2)).print()
    //输出case class
    //输出csv文件的两列数据 name,age , includedFields = 0,1
    case class MyCaseClass(name:String,age:Int)
    env.readCsvFile[MyCaseClass](filePath,ignoreFirstLine = true,includedFields = Array(0,1)).print()

    //POJO,使用java的类
    env.readCsvFile[Person](filePath,ignoreFirstLine = true,pojoFields = Array("name","age","work")).print()
    /** output:
     * (Bob,32,Developer)
     * (Jorge,30,Developer)
     */
  }

  //从本地文件夹读取数据，并打印
  def readTextDir(env:ExecutionEnvironment):Unit = {
    val filePath = "c:\\flink\\input" //注意,这是文件夹
    env.readTextFile(filePath).print()//输出文件夹里面所有的文件内容
  }
  //从本地文件读取数据，并打印
  def readTextFile(env:ExecutionEnvironment):Unit = {
    val filePath = "c:\\flink\\hello.txt"
    env.readTextFile(filePath).print()
  }
  //从本地集合读取数据，并打印
  def fromCollection(env:ExecutionEnvironment):Unit = {

    val data = 1 to 10
    env.fromCollection(data).print()
  }
}
