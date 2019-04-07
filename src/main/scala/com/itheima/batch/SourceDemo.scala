package com.itheima.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

//不同的数据源类型Demo
object SourceDemo {
  //1,张三,1,98
  case class Student(id:Int,name:String)

  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建source
    //1.基于本地集合的source
    //    val localSource: DataSet[String] = env.fromCollection(
    //      List("hadoop hive flume spark", "liyuxin love eat apple")
    //    )

    //    val flatSet: DataSet[String] = localSource.flatMap(x=>x.split(" "))

    //2.基于本地文件的source
    //    val textFileSource: DataSet[String] = env.readTextFile("data/input/wordcount.txt")
    //    textFileSource.print()

    //3.基于HDFS的source
//    val hdfsSource: DataSet[String] = env.readTextFile("hdfs://node001:8020/test/input/wordcount.txt")
//    hdfsSource.print()

      //4.读取CSV数据文件
//    val csvSource: DataSet[Student] = env.readCsvFile[Student]("data/input/subject.csv")
//    csvSource.print()

    //5.读取压缩文件
    val gzSource: DataSet[String] = env.readTextFile("data/input/wordcount.txt.gz")
    gzSource.print()
  }
}
