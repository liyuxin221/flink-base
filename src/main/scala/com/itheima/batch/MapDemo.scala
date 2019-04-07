package com.itheima.batch

import  org.apache.flink.api.scala._
//transformation:map
object MapDemo {

  case class User(id:Int,name:String)
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val source: DataSet[String] = env.fromCollection(
      List("1,张三", "2,李四", "3,王五", "4,赵六")
    )

    //transformation
    val userDataSet: DataSet[User] = source.map {
      x =>
        val fieldArr: Array[String] = x.split(",")
        User(fieldArr(0).toInt, fieldArr(1))
    }
    userDataSet.print()
  }
}
