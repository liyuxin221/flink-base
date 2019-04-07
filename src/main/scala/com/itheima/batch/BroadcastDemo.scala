package com.itheima.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val studentDataSet = env.fromCollection(List((1, "张三"), (2, "李四"), (3, "王五")))
    val scoreDataSet = env.fromCollection(List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86)))

    val result: DataSet[(String, String, Int)] = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {

      //初始化赋值.
      var list: List[(Int, String)] = _

      override def open(parameters: Configuration): Unit = {

        //导入隐式转换
        import scala.collection.JavaConverters._

        list = getRuntimeContext.getBroadcastVariable[(Int, String)]("student").asScala.toList
      }

      override def map(value: (Int, String, Int)): (String, String, Int) = {
        val studentId: Int = value._1

        val studentName: String = list.filter(_._1 == studentId)(0)._2

        (studentName, value._2, value._3)

      }
    }).withBroadcastSet(studentDataSet, "student")
    result.print()


  }
}
