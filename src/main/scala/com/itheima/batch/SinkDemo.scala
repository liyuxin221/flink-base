package com.itheima.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

//sink:下沉地 数据输出到哪里去
object SinkDemo {
  def main(args: Array[String]): Unit = {
    //获取flink批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val wordDataSet: DataSet[String] = env.fromCollection(
      List("hadoop", "hadoop", "hadoop", "hive", "hive", "spark",
        "spark", "flink")
    )


    //TODO:transformation

    //本地集合的sink
//    val localSeq: Seq[String] = wordDataSet.collect()
//    localSeq.foreach(println)

      //基于本地文件的sink
//    wordDataSet.writeAsText("data/output/1.txt")

        //基于hdfs的sink
    wordDataSet.writeAsText("hdfs://node001:8020/test/ouput/1.txt")
    env.execute("SinkDemoAPP")


  }
}
