package com.imooc.flink.scala.course04

import scala.util.Random

/**
 * 数据库的工具类
 */
object DBUtils {

  def getConection():String = {
    new Random().nextInt(10) + ""
  }
  def returnConnection(connection:String):Unit={

  }
}
