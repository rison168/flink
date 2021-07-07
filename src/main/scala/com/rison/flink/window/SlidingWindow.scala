package com.rison.flink.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author : Rison 2021/7/7 下午6:02
 *         滑动窗口
 *         滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参
 *         数，一个是 window_size，一个是 sliding_size。
 *         下面代码中的 sliding_size 设置为了 5s，也就是说，每 5s 就计算输出结果一次，
 *         每一次计算的 window 范围是 15s 内的所有元素
 */
object SlidingWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 8888)
    dataStream
      .map(
        data => {
          val strings: Array[String] = data.split(",")
          (strings(0).toString, strings(1).toLong, strings(2).toDouble)
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce(
        (x, y) => (x._1, x._2, x._3.min(y._3))
      ).print()

    //
    env.execute("Tumbling window")
  }
}
