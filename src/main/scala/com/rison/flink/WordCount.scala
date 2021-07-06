package com.rison.flink

import org.apache.flink.api.scala._

/**
 * @author : Rison 2021/7/6 上午8:53
 *         批处理的wordCount
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath = "data/hello.txt"
    val inputDS: DataSet[String] = env.readTextFile(inputPath)

    //分词之后，对单词进行groupBy分组，然后进行sum聚合
    val wordCountDS = inputDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wordCountDS.print()

//    env.execute(" word count")
  }
}
