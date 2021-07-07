package com.rison.flink.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author : Rison 2021/7/7 下午6:03
 *         滚动窗口
 *         Flink 默认的时间窗口根据 Processing Time 进行窗口的划分，将 Flink 获取到的
 *         数据根据进入 Flink 的时间划分到不同的窗口中
 */
object TumblingWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    dataStream
      .map(
        data => {
          val strings: Array[String] = data.split(",")
          (strings(0).toString, strings(1).toLong, strings(2).toDouble)
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce(
        (x, y) => (x._1, x._2, x._3.min(y._3))
      ).print()
    env.execute("Tumbling window")
  }
}
