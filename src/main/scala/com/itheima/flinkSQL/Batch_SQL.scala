package com.itheima.flinkSQL

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

//flink SQLDEMO
object Batch_SQL {

  case class Order(id: Int, userName: String, time: String, money: Double)

  def main(args: Array[String]): Unit = {
    //构建table运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //构建数据源
    val orderDataSet: DataSet[Order] = env.fromElements(
      Order(1, "zhangsan", "2018-10-20 15:30", 358.5),
      Order(2, "zhangsan", "2018-10-20 16:30", 131.5),
      Order(3, "lisi", "2018-10-20 16:30", 127.5),
      Order(4, "lisi", "2018-10-20 16:30", 328.5),
      Order(5, "lisi", "2018-10-20 16:30", 432.5),
      Order(6, "zhaoliu", "2018-10-20 22:30", 451.0),
      Order(7, "zhaoliu", "2018-10-20 22:30", 362.0),
      Order(8, "zhaoliu", "2018-10-20 22:30", 364.0),
      Order(9, "zhaoliu", "2018-10-20 22:30", 341.0)
    )

    //将DataSet注册为一张表
    tableEnv.registerDataSet("table1", orderDataSet)

    //使用table运行环境的sqlQuery方法来执行SQL语句
    //case class Order(id:Int,userName:String,time:String,money:Double)
    //统计用户消费订单的总金额、最大金额、最小金额、订单总数
    val allOrderTable = tableEnv.sqlQuery{
      """
        |select
        | userName,
        | count(1) as totalCount,
        | max(money) as maxMoney,
        | min(money) as minMoney,
        | sum(money) as totalMoney
        |from table1
        |group by
        | userName
      """.stripMargin}

    allOrderTable.printSchema()

    tableEnv.toDataSet[Row](allOrderTable).print()


  }
}
