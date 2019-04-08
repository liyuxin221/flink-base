package com.itheima.streaming

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

import org.apache.flink.api.scala._

import scala.util.Random

//水印机制:
object WaterMarkDemo {

  //创建一个订单样例类Order，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  //时间戳即是eventtime
  case class Order(id: String, userId: Int, money: Long, timestamp: Long)

  def main(args: Array[String]): Unit = {
    //创建流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //创建一个自定义数据源

    val orderDataStream: DataStream[Order] = env.addSource(new RichSourceFunction[Order] {
      var flag = true

      //随机生成订单ID（UUID）
      //随机生成用户ID（0-2）
      //随机生成订单金额（0-100）
      //时间戳为当前系统时间
      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        while (flag) {
          val order = Order(UUID.randomUUID().toString, Random.nextInt(3), Random.nextInt(100), new java.util.Date().getTime)
          ctx.collect(order)

          //每隔1秒生成一个订单
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        flag = false
      }
    })


    val watermarkDataStream: DataStream[Order] = orderDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Order] {

      //添加水印
      //允许延迟2秒
      var currentTimestamp = 0L
      val delayTime = 2000

      //在获取水印方法中，打印水印时间、事件时间和当前系统时间
      override def getCurrentWatermark: Watermark = {
        val watermark = new Watermark(currentTimestamp - delayTime)
        val dateFormat = FastDateFormat.getInstance("HH:mm:ss")

        println(s"当前水印时间:${dateFormat.format(watermark.getTimestamp)}," +
          s"当前时间时间:${dateFormat.format(currentTimestamp)}," +
          s"当前系统时间:${dateFormat.format(System.currentTimeMillis())}")
        watermark
      }

      override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
        val timestamp: Long = element.timestamp
        currentTimestamp = Math.max(currentTimestamp, timestamp)
        currentTimestamp
      }
    })

    watermarkDataStream
      //按照用户进行分流
      .keyBy(_.userId)
      //设置5秒的时间窗口
      .timeWindow(Time.seconds(5))
      //进行聚合计算
      .reduce {
      (o1, o2) => Order(o1.id, o1.userId, o1.money + o2.money, 0)
      //打印结果数据
    }.print()


    //启动执行流处理
    env.execute("WaterMarkDemoJob")
  }
}
