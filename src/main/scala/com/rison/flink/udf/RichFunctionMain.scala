package com.rison.flink.udf

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/7/7 上午9:07
 *         RichFunction
 *
 */
object RichFunctionMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(
      List(2, 2, 10, 11, 12, 14, 16, 17, 20)
    ).flatMap(new MyFlatMap).print()
    env.execute("rich function")

  }
}

class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {
  var subTaskIndex = 0

  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
    //以下可以做些初始化操作，比如建立hdfs连接等等
  }

  override def flatMap(in: Int, collector: Collector[(Int, Int)]): Unit = {
    if (in % 2 == subTaskIndex) {
      collector.collect((subTaskIndex, in))
    }
  }

  override def close(): Unit = {
    //做一些关闭清理操作
  }
}
