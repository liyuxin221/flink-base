package com.itheima.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

//transformation:reduce
object ReduceDemo {
  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val source: DataSet[(String, Int)] = env.fromCollection(
      List(("java", 1), ("java", 1), ("java", 1))

    )
    val result: DataSet[(String, Int)] = source.reduce {
      (t1, t2) =>
        (t2._1, t1._2 + t2._2)
    }
    result.print()
  }
}
