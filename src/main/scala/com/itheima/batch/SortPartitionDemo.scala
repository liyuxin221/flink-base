package com.itheima.batch

import org.apache.flink.api.scala._
object SortPartitionDemo {


  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建源数据
    val wordDataSet: DataSet[String] = env.fromCollection(
      List("hadoop", "hadoop", "hadoop", "hive", "hive", "spark",
        "spark", "flink")
    ).setParallelism(2)

    val sortedDataSet: DataSet[String] = wordDataSet.sortPartition(_.toString,org.apache.flink.api.common.operators.Order.DESCENDING)

    sortedDataSet.writeAsText("data/sort_output")

    env.execute("SortApp")
  }
}
