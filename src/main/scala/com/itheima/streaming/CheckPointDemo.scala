package com.itheima.streaming


import java.util
import java.util.concurrent.TimeUnit

import org.apache.commons.lang.RandomStringUtils
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

//checkpoint实现
object CheckPointDemo {

  //创建一个订单样例类（订单ID、用户ID、订单金额）
  case class Order(id: Int, userID: String, money: Double)

  def main(args: Array[String]): Unit = {
    //获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //配置checkpoint
    // 每1秒开启设置一个checkpoint
    env.enableCheckpointing(5000)
    // 设置checkpoint快照保存到HDFS
    env.setStateBackend(new FsStateBackend("hdfs://node001:8020/flink-checkpoint"))
    // 设置checkpoint的执行模式，最多执行一次或者至少执行一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置checkpoint的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //设置同一时间有多少 个checkpoint可以同时执行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)


    //添加一个添加自动生成订单数据的source
    val orderDataStream: DataStream[(Int, String, Double)] = env.addSource(new RichSourceFunction[(Int, String, Double)] {
      var isRunning = true

      //每秒生成一个
      override def run(ctx: SourceFunction.SourceContext[(Int, String, Double)]): Unit = {
        while (isRunning) {
          for (i <- 0 until 1000) {
            ctx.collect(
              i, "zhangsan", RandomStringUtils.randomNumeric(3).toDouble
            )
            TimeUnit.SECONDS.sleep(5)
          }
        }
      }

      //使用一个变量控制停止生成数据
      override def cancel(): Unit =
      {isRunning = false}
    })

    //使用keyBy按照用户名分流
    //使用timeWindow划分时间窗口
    //使用apply进行自定义计算
    orderDataStream.keyBy(_._2).timeWindow(Time.seconds(3)).apply(new WindowReduceRichFunction).print()

    //运行测试

    //查看HDFS中是否已经生成checkpoint数据
    env.execute("checkpointApp")
  }

  //创建一个UDFState，用户保存checkpoint的数据
  case class UDFState(var totalMoney:Double)

  //创建一个类实现WindowFunction，并实现ListedCheckpoint
  class WindowReduceRichFunction extends WindowFunction[(Int,String,Double),Double,String,TimeWindow] with
    ListCheckpointed[UDFState] {

    var totalMoney=0.0
    //在apply方法中实现累加
    override def apply(key: String, window: TimeWindow, input: Iterable[(Int, String, Double)], out: Collector[Double]): Unit = {
      for(elem <-input){
        totalMoney+=elem._3
      }
      out.collect(totalMoney)
    }

    //在snapshotState实现快照存储数据，在restoreState实现从快照中恢复数据
    override def snapshotState(checkpointId: Long, timestamp: Long): util.List[UDFState] = {
      val states = new util.ArrayList[UDFState]()
      states.add(UDFState(totalMoney))
      states
    }

    override def restoreState(state: util.List[UDFState]): Unit = {
      totalMoney=state.get(0).totalMoney
    }
  }
}
