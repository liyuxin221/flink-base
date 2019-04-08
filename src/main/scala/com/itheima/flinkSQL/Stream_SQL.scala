package com.itheima.flinkSQL

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

import scala.util.Random

//流数据SQL
object Stream_SQL {

  case class Order(id: String, userID: Int, money: Int, time: Long)

  def main(args: Array[String]): Unit = {
    // 获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取Table运行环境
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    // 设置处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建一个订单样例类Order，包含四个字段（订单ID、用户ID、订单金额、时间戳）
    // 创建一个自定义数据源
    // 每隔1秒生成一个订单
    // 添加水印，允许延迟2秒
    val waterMarkDataStream: DataStream[Order] = env.addSource(new mySource).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Order]() {
      var currentTimestamp = 0L

      val timeInterval = 2000L

      //获取水印时间
      override def getCurrentWatermark: Watermark = {
        val watermark = new Watermark(currentTimestamp - timeInterval)
        watermark
      }

      //抽取eventTimeStamp
      override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {

        currentTimestamp = Math.max(element.time, currentTimestamp)

        currentTimestamp
      }
    })

    // 导入import org.apache.flink.table.api.scala._隐式参数
    import  org.apache.flink.table.api.scala._

    // 使用registerDataStream注册表，并分别指定字段，还要指定rowtime字段
    tableEnv.registerDataStream("streamTable",waterMarkDataStream,'id,'userID,'money,'ordertime.rowtime)

    // 编写SQL语句统计用户订单总数、最大金额、最小金额
    // 使用tableEnv.sqlQuery执行sql语句
    // 分组时要使用tumble(时间列, interval '窗口时间' second)来创建窗口
    val result=tableEnv.sqlQuery(
      """
        |select
        | userID,
        | count(1) as totalCount,
        | max(money) as maxMoney,
        | min(money) as minMoney,
        | sum(money) as totalMoney
        |from streamTable
        |group by
        |userID,
        |tumble(ordertime, interval '5' second)
      """.stripMargin)

    // 将SQL的执行结果转换成DataStream再打印出来
    val finalResult: DataStream[Row] = tableEnv.toAppendStream[Row](result)
    finalResult.print()
    // 启动流处理程序
    env.execute("streamSQL")
  }

  class mySource extends RichSourceFunction[Order]() {

    var flag = true

    // 使用for循环生成1000个订单
    // 随机生成订单ID（UUID）
    // 随机生成用户ID（0-2）
    // 随机生成订单金额（0-100）
    // 时间戳为当前系统时间
    override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
      while (flag) {
        for (i <- 1 until 1000) {
          val order = Order(UUID.randomUUID().toString, Random.nextInt(3), Random.nextInt(101), System.currentTimeMillis())
          ctx.collect(order)
          TimeUnit.SECONDS.sleep(1)
        }
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }

}
