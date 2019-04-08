package com.itheima.streaming

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector

//apply方法 实现
object ApplyDemo {
  def main(args: Array[String]): Unit = {
    //获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //构建socket流数据源，并指定IP地址和端口号
    val source: DataStream[String] = env.socketTextStream("node001",9999)
    //对接收到的数据转换成单词元组
    val wordDataStream: DataStream[(String, Int)] = source.flatMap(_.split(" ")).map(_->1)
    //使用keyBy进行分流（分组）
//    val groupedStream: KeyedStream[(String, Int), Tuple] = wordDataStream.keyBy(0)
    val groupedStream: KeyedStream[(String, Int), String] = wordDataStream.keyBy(_._1)
    
    //使用timeWinodw指定窗口的长度（每3秒计算一次）
    val windowDataStream= groupedStream.timeWindow(Time.seconds(5))

    //实现一个WindowFunction匿名内部类
    val resultDataStream: DataStream[(String, Int)] = windowDataStream.apply(new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {

      //在apply方法中实现聚合计算
      //使用Collector.collect收集数据
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val resultWordCount: (String, Int) = input.reduce {
          (t1, t2) =>
          (t1._1, t1._2 + t2._2)
        }

        out.collect(resultWordCount)
      }
    })

    //打印输出
    resultDataStream.print()

    //启动执行
    env.execute("applydemo")

    //在Linux中，使用nc -lk 端口号监听端口，并发送单词
  }
}
