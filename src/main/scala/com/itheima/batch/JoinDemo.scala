package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

//transformation
object JoinDemo {

  case class Subject(id: Int, name: String)

  case class Score(id: Int, studentName: String, subjectId: Int,
                   score: Double)

  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //构建数据源
    val subjectDataSet =
      env.readCsvFile[Subject]("./data/input/subject.csv")
    val scoreDataSet =
      env.readCsvFile[Score]("./data/input/score.csv")

    //transformation
    scoreDataSet.join(subjectDataSet).where(_.subjectId).equalTo(_.id).print()
  }
}
