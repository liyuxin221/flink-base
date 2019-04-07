package com.itheima.batch

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.io.Source

object DistributionCacheDemo {

  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //创建成绩数据集
    val scoreDataSet = env.fromCollection(List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86)))

    env.registerCachedFile("hdfs://node001:8020/distribute_cache_student", "student")

    //对成绩进行map转换,将（学生ID, 学科, 分数）转换为（学生姓名，学科，分数）
    val result: DataSet[(String, String, Int)] = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
      var list: List[(Int, String)] = _

      override def open(parameters: Configuration): Unit = {

        val file: File = getRuntimeContext.getDistributedCache.getFile("student")
        val lineIter: Iterator[String] = Source.fromFile(file).getLines()

        list = lineIter.map {
          line =>
            val fieldArr: Array[String] = line.split(",")
            (fieldArr(0).toInt, fieldArr(1))
        }.toList
      }

      override def map(value: (Int, String, Int)): (String, String, Int) = {
        val studentId: Int = value._1
        val studentName: String = list.filter(_._1 == studentId)(0)._2

        (studentName, value._2, value._3)
      }
    })
    result.print()

  }
}
