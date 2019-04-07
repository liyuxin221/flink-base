package com.itheima.batch

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.api.scala._

//transformation:aggregate
object AggregateDemo {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val source: DataSet[(String, Int)] = env.fromCollection(
      List(("java", 1), ("java", 1), ("scala", 1))
    )
    val groupedDataSet: GroupedDataSet[(String, Int)] = source.groupBy(0)
    val resultDataSet: AggregateDataSet[(String, Int)] = groupedDataSet.aggregate(Aggregations.SUM,1)
    resultDataSet.print()
  }
}
