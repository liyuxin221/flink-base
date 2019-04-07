package com.itheima.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._


//transformation:filter
object FilterMap {

  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val wordDataSet: DataSet[String] = env.fromCollection(
      List(
        "hadoop", "hive", "spark", "flink"
      )
    )
    val result: DataSet[String] = wordDataSet.filter(_.startsWith("ha"))

    result.print()
  }
}
