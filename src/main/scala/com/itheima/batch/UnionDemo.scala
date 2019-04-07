package com.itheima.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import org.apache.flink.api.scala._

//transformation:union
object UnionDemo {
  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建源数据
    val textDataSet1: DataSet[String] = env.fromCollection(
      List(
        "hadoop", "hive", "flume"
      )
    )
    val textDataSet2 = env.fromCollection(List(
      "hadoop", "hive", "spark"
    ))


//    textDataSet1.union(textDataSet2).print()

  }
}
