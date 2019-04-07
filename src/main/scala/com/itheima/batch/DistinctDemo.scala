package com.itheima.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * @author: Liyuxin wechat:13011800146.
  * @Title: DistinctDemo
  * @ProjectName flink-base
  * @date: 2019/4/6 23:10
  * @description:
  */
object DistinctDemo {
  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建源数据
    val tupleDataSet = env.fromCollection(
      List(("java", 1), ("java", 1), ("scala", 1))
    )

    tupleDataSet.distinct(0).print()

  }
}
