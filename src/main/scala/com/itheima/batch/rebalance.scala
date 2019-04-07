package com.itheima.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

//transformation:rebalance
object rebalance {
  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val numDataSet: DataSet[Long] = env.generateSequence(0,100)

    val filterDataSet: DataSet[Long] = numDataSet.filter(_ > 8).rebalance()

    //使用map操作传入RichMapFunction,将当前子任务的ID和数字构成一个元组
    val result: DataSet[(Long, Long)] = filterDataSet.map(new RichMapFunction[Long, (Long, Long)] {
      override def map(in: Long): (Long, Long) = {
        (getRuntimeContext.getIndexOfThisSubtask, in)
      }
    })
    result.print()
  }
}
