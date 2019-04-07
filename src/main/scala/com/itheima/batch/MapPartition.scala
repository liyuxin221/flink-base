package com.itheima.batch

import org.apache.flink.api.scala._

//transformation
object MapPartition {
  case class User(id:Int,name:String)
  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val textDataSet = env.fromCollection(List(
      "1,张三", "2,李四", "3,王五", "4,赵六"
    ))

    val userDataSet: DataSet[User] = textDataSet.mapPartition {
      iter =>
        iter.map {
          elem =>
            val fieldArr: Array[String] = elem.split(",")
            User(fieldArr(0).toInt, fieldArr(1))
        }
    }
    userDataSet.print()
  }
}
