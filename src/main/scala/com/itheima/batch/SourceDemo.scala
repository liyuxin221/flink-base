package com.itheima.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import org.apache.flink.api.scala._

//不同的数据源类型Demo
object SourceDemo {
  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建source
    //1.基于本地集合的source
//    val localSource: DataSet[String] = env.fromCollection(
//      List("hadoop hive flume spark", "liyuxin love eat apple")
//    )

//    val flatSet: DataSet[String] = localSource.flatMap(x=>x.split(" "))

    //2.基于


    val wordsSet: DataSet[(String, Int)] = flatSet.map(x=>(x,1))
    val groupWords: GroupedDataSet[(String, Int)] = wordsSet.groupBy(0)
    val wordCountSet: AggregateDataSet[(String, Int)] = groupWords.sum(1)

    wordCountSet.collect().foreach(println)


  }
}
