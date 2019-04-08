package com.itheima.streaming

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

//入门案例
object StreamingDemo {
  def main(args: Array[String]): Unit = {
    //获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    //构建socket流数据源，并指定IP地址和端口号
    val socketDataStream: DataStream[String] = env.socketTextStream("node001", 9999)

    //对接收到的数据转换成单词元组
    val wordAndCountDataStream: DataStream[(String, Int)] = socketDataStream.flatMap(_.split(" ")).map(_-> 1)
    //使用keyBy进行分流（分组）
    val groupedDataStream: KeyedStream[(String, Int), String] = wordAndCountDataStream.keyBy(_._1)
    //使用timeWinodw指定窗口的长度（每5秒计算一次）
    val groupedWindowDataStream: WindowedStream[(String, Int), String, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(5))
    //使用sum执行累加
    val windowedDataStream: DataStream[(String, Int)] = groupedWindowDataStream.reduce {
      (t1, t2) =>
        (t1._1, t1._2 + t2._2)
    }
    //打印输出
    windowedDataStream.print()
    //启动执行
    env.execute("StreamDemo")
    //在Linux中，使用nc -lk 端口号监听端口，并发送单词

  }
}
