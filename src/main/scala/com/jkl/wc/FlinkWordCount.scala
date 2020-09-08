package com.jkl.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object FlinkWordCount {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "E:\\workspace\\Flink_Learning\\src\\main\\resources\\test.txt"

    val inputDataSet = environment.readTextFile(inputPath)

    val res = inputDataSet
      .flatMap(_.split("\t"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    res.print()

  }

}
