package com.imooc.flink.scala.course02

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * 使用Scala开发Flink的批处理应用程序
 */
object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {
    //读取本地磁盘的文件
    //读取本地磁盘的文件
    /** hello.txt:
     * hello	world	weclome
     * hi	world	hello
     * weclome	keke
     */
    val filePath = "e:\\flink\\hello.txt";
    //1,获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2,读取数据
    val text = env.readTextFile(filePath)
    //隐式转换
    import org.apache.flink.api.scala._
    //3,transform 调用算子
    //以下是, 读取每一行，按\t分隔，返回(word,1)
    text.flatMap(_.toLowerCase.split("\t"))//读取每一行，按\t分隔，
      .filter(_.nonEmpty)//不是空字符串
      .map((_,1))//返回(word,1)
      .groupBy(0)//按单词分组,0 是指 (word,1)的word
      .sum(1)//统计相同单词的总数,1 是指 (word,1)的1
      .print()//在控制台打印
    /** output:
     * (weclome,2)
     * (world,2)
     * (hello,2)
     * (hi,1)
     * (keke,1)
     */
  }
}
