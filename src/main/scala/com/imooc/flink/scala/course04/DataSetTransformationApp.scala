package com.imooc.flink.scala.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
 * 参考文档
 * https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/batch/dataset_transformations.html#join
 */
object DataSetTransformationApp {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //mapFunction(env)
    //filterFunction(env)
//    mapPartitionFunction(env)
//    firstFunction(env)
//    flatMapFunction(env)
//    distinctFunction(env)
//    joinFunction(env)
//    leftOuterJoinFunction(env)
//    rightOuterJoinFunction(env)
//    fullOuterJoinFunction(env)
    crossFunction(env)
  }
  //迪卡尔 积
  def crossFunction(env: ExecutionEnvironment):Unit = {
    val info1 = List("曼联","曼城")
    val info2 = List(3,1,0)
    //隐式转换
    import org.apache.flink.api.scala._
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()

    /** output:
     * (曼联,3)
     * (曼联,1)
     * (曼联,0)
     * (曼城,3)
     * (曼城,1)
     * (曼城,0)
     */
  }

  //fullOuterJoin 全匹配,右表 ,左表可能有null值
  def fullOuterJoinFunction(env: ExecutionEnvironment):Unit = {
    val info1 = ListBuffer[(Int,String)]()//编号,昵称
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"小队长"))
    info1.append((4,"猪头呼"))

    val info2 = ListBuffer[(Int,String)]()//编号,城市
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))
    //隐式转换
    import org.apache.flink.api.scala._
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)


    data1.fullOuterJoin(data2)
      .where(0).equalTo(0)  // 类似sql语句的 ON data1.id = data2.id
      .apply((first,second)=>{
        if(first == null) (second._1,"-",second._2) else if (second == null) (first._1,first._2,"-") else (first._1,first._2,second._2)
      }).print()

    /** output:
     * (3,小队长,成都)
     * (1,PK哥,北京)
     * (5,-,杭州)
     * (2,J哥,上海)
     * (4,猪头呼,-)
     */
  }
  //rightOuterJoin 右匹配,以右表为基准,左表可能有null值
  def rightOuterJoinFunction(env: ExecutionEnvironment):Unit = {
    val info1 = ListBuffer[(Int,String)]()//编号,昵称
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"小队长"))
    info1.append((4,"猪头呼"))

    val info2 = ListBuffer[(Int,String)]()//编号,城市
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))
    //隐式转换
    import org.apache.flink.api.scala._
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    //rightJoin 右匹配,以右表为基准,左表可能有null值
    data1.rightOuterJoin(data2)
      .where(0).equalTo(0)  // 类似sql语句的 ON data1.id = data2.id
      .apply((first,second)=>{
        if(first != null) // first 不为null
          (first._1,first._2,second._2) //返回tuple3
        else
          (second._1,"-",second._2) //返回tuple3
      }).print()

    /** output:
     * (3,小队长,成都)
     * (1,PK哥,北京)
     * (5,-,杭州)
     * (2,J哥,上海)
     */
  }
  //leftOuterJoin 左匹配,以左表为基准,右表可能有null值
  def leftOuterJoinFunction(env: ExecutionEnvironment):Unit = {
    val info1 = ListBuffer[(Int,String)]()//编号,昵称
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"小队长"))
    info1.append((4,"猪头呼"))

    val info2 = ListBuffer[(Int,String)]()//编号,城市
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))
    //隐式转换
    import org.apache.flink.api.scala._
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    //leftOuterJoin 左匹配,右表可能有null值
    data1.leftOuterJoin(data2)
      .where(0).equalTo(0)  // 类似sql语句的 ON data1.id = data2.id
      .apply((first,second)=>{
        if(second != null) // second 不为null
          (first._1,first._2,second._2) //返回tuple3
        else
          (first._1,first._2,"-") //返回tuple3
      }).print()

    /** output:
     * (3,小队长,成都)
     * (1,PK哥,北京)
     * (2,J哥,上海)
     * (4,猪头呼,-)
     */
  }
  //join 全匹配
  def joinFunction(env: ExecutionEnvironment):Unit = {
    val info1 = ListBuffer[(Int,String)]()//编号,昵称
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"小队长"))
    info1.append((4,"猪头呼"))

    val info2 = ListBuffer[(Int,String)]()//编号,城市
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))
    //隐式转换
    import org.apache.flink.api.scala._
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    // join 全匹配
    data1.join(data2)
      .where(0).equalTo(0)  // 类似sql语句的 ON data1.id = data2.id
      .apply((first,second)=>{
        (first._1,first._2,second._2) //返回tuple3
      }).print()

    /** output:
     * (3,小队长,成都)
     * (1,PK哥,北京)
     * (2,J哥,上海)
     */
  }
  //distinct  去重
  def distinctFunction(env: ExecutionEnvironment):Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")
    //隐式转换
    import org.apache.flink.api.scala._
    val data = env.fromCollection(info)
    data.flatMap(_.split(",")).distinct().print()

    /** output:
     * hadoop
     * flink
     * spark
     */
  }
  //flatMap
  def flatMapFunction(env: ExecutionEnvironment):Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")
    //隐式转换
    import org.apache.flink.api.scala._
    val data = env.fromCollection(info)
    data.print()
    //map满足不了与split的结合用法
    data.map(_.split(",")).print() //output: [Ljava.lang.String;@3a5237
    data.flatMap(_.split(",")).print() //output: hadoop \n spark \n .....
    //经典用法
    data
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()

    /**
     * (hadoop,2)
     * (flink,3)
     * (spark,1)
     */
  }
  //first
  def firstFunction(env: ExecutionEnvironment):Unit = {
    //隐式转换
    import org.apache.flink.api.scala._
    val info = ListBuffer[(Int,String)]()
    info.append((1,"Hadoop"))
    info.append((1,"Spart"))
    info.append((1,"Flink"))
    info.append((2,"Java"))
    info.append((2,"String Boot"))
    info.append((3,"Linux"))
    info.append((4,"Vue.js"))

    val data = env.fromCollection(info)

    data.first(3).print()
    /** output:
     * (1,Hadoop)
     * (1,Spart)
     * (1,Flink)
     */
    data.groupBy(0).first(2).print()
    /** output:
     * (3,Linux)
     * (1,Hadoop)
     * (1,Spart)
     * (2,Java)
     * (2,String Boot)
     * (4,Vue.js)
     */
    data.groupBy(0)
      .sortGroup(1,Order.DESCENDING)//按字母降序
      .first(2).print()

    /** output:
     * (3,Linux)
     * (1,Spart)
     * (1,Hadoop)
     * (2,String Boot)
     * (2,Java)
     * (4,Vue.js)
     */
  }
  //mapPartition
  def mapPartitionFunction(env: ExecutionEnvironment):Unit = {
    //隐式转换
    import org.apache.flink.api.scala._
  val students = new ListBuffer[String]
    for(i<-1 to 100){
      students.append("student: "+ i)
    }
    val data = env.fromCollection(students)
    //使用map 操作数据库,会产生很多数据库链接,与数据源的元素对应
    data.map(x => {
      //每一个元素要存储到数据库,肯定需要先获取一个connection
      val connection = DBUtils.getConection()
      println("------map connection: "+ connection + " -------")
      //TODO:保存到数据库,省略
      DBUtils.returnConnection(connection)
      x
    }).print()

    //使用mapPartition 操作数据库,数据库链接的产生
    data.mapPartition(x => {
      //每一个元素要存储到数据库,肯定需要先获取一个connection
      val connection = DBUtils.getConection()
      println("------mapPartition connection: "+ connection + " -------")
      //TODO:保存到数据库,省略
      DBUtils.returnConnection(connection)
      x
    })
      .setParallelism(2)//控制mapPartition算子里有多少个Db connection
      .print()
  }
  def filterFunction(env: ExecutionEnvironment): Unit = {
    //隐式转换
    import org.apache.flink.api.scala._
    val list = 1 to 10  //List(1,2,3,4,5,6,7,8,9,10)
    val data = env.fromCollection(list)
    //map -> filter -> map 链式调用
    data.map(_ + 1).filter(_>5).print()
    data.map(_ + 1).filter(_>5).map(_ + 1).print()
  }

  //map
  def mapFunction (env:ExecutionEnvironment) :Unit = {
    //隐式转换
    import org.apache.flink.api.scala._
    val list = 1 to 10  //List(1,2,3,4,5,6,7,8,9,10)
    val data = env.fromCollection(list)
    //方式一
    data.map((x:Int)=>x+1).print()
    //方式二
    data.map((x)=>x+1).print()
    //方式三
    data.map(x=>x+1).print()
    //方式四
    data.map(_ + 1).print()
  }
}
