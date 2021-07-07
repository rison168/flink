package com.rison.flink.udf

import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/7/7 上午8:54
 *         lambada Function instance
 */
object LambdaFunctionMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.fromCollection(
      List("flink", "spark")
    )
    dataStream.filter(_.contains("flink")).print("lambda:")
    env.execute("lambda Function")
  }
}
