package com.itheima.batch

import org.apache.flink.api.scala._

object HashPartitionDemo {
  def main(args: Array[String]): Unit = {

    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(2)

    //构建数据源
    val numDataSet = env.fromCollection(
      List(1,1,1,1,1,1,1,2,2,2,2,2)
    )

    val partitioned: DataSet[Int] = numDataSet.partitionByHash(x=>x)

    partitioned.writeAsText("data/partition_output")

    partitioned.print()
  }
}
