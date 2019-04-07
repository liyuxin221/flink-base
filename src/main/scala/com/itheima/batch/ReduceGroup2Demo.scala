package com.itheima.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.api.scala._

//transformation:reduceGroup
object ReduceGroup2Demo {
  def main(args: Array[String]): Unit = {
    //获取批处理的运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val source: DataSet[(String, Int)] = env.fromCollection(
      List(
        ("java", 1), ("java", 1), ("scala", 1)
      )
    )
    val groupedDataSet: GroupedDataSet[(String, Int)] = source.groupBy(0)
    val result: DataSet[(String, Int)] = groupedDataSet.reduceGroup {
      iter =>
        iter.reduce {
          (t1, t2) => (t1._1, t1._2 + t2._2)
        }
    }
    result.print()
  }
}
