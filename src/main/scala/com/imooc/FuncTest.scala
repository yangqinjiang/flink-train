package com.imooc

object FuncTest {
  def main(args: Array[String]): Unit = {
    val tiger = new Tiger
    println(tiger)
    val tiger2 =test01(10,tiger)
    println(tiger2 == tiger) //同一个对象
    println(tiger2.hashCode() == tiger.hashCode()) //同一个对象

    //
    sum(1,2)
    //f6("v2")//compile error
    f6(p2="v2")
  }

  def f6(p1:String = "v1",p2:String): Unit ={
    println(p1+p2)
  }
  def sum(n1:Int,n2:Int):Int  = {
    n1 + n2
    // 有return , 应该明确指定返回类型,不能省略
  }
  //如果返回值这里什么都没写,即表示该函数没有返回值,
  //这时return 无效
  //实际返回 ()
  def sum2(n1:Int,n2:Int)  {
    n1+n2
  }
  def test01(n1:Int,tiger:Tiger):Tiger = {
    println("n1=" +n1)
    tiger.name = "jack"
//    tiger = new Tiger //compile error
    tiger
  }
}
class Tiger {
  var name = ""
}