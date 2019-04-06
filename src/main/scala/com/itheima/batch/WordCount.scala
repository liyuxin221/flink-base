package com.itheima.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import org.apache.flink.api.scala._

// flink入门程序
object WordCount {
  def main(args: Array[String]): Unit = {
    //获取flink 批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建source
    //表示从本地内存中的一个集合来构建一个Flink能够处理的数据源
    //导入隐式转换
    val wordDataSet: DataSet[String] = env.fromCollection(
      List("hadoop hive spark", "flink mapreduce hadoop hive", "flume spark spark hive")
    )

    val words: DataSet[String] = wordDataSet.flatMap(_.split(" "))

    val wordNumDataSet: DataSet[(String, Int)] = words.map(_->1)

    val wordGroupDataSet: GroupedDataSet[(String, Int)] = wordNumDataSet.groupBy(0)

    val wordCountDataSet= wordGroupDataSet.sum(1)

    wordCountDataSet.print()


  }
}
