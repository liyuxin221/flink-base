package com.itheima.batch

import org.apache.flink.api.scala._

import scala.collection.mutable

//transformation:flatMap
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val source: DataSet[String] = env.fromCollection(
      List(
        "张三,中国,江西省,南昌市",
        "李四,中国,河北省,石家庄市",
        "Tom,America,NewYork,Manhattan"
      )
    )

    val result: DataSet[(String, String)] = source.flatMap {
      x =>
        val fieldArr: mutable.ArrayOps[String] = x.split(",")
        List(
          (fieldArr(0), fieldArr(1)),
          (fieldArr(0), fieldArr(1) + fieldArr(2)),
          (fieldArr(0), fieldArr(1) + fieldArr(2) + fieldArr(3))
        )
    }
    result.print()
  }
}
