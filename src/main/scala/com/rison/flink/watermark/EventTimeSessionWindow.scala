package com.rison.flink.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author : Rison 2021/7/8 下午2:41
 *         会话窗口
 */
object EventTimeSessionWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val dataMap: DataStream[(String, Double, Long)] = dataStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        (arr(0).toString, arr(1).toDouble, arr(2).toLong)
      }
    )
    dataMap.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Double, Long)](Time.milliseconds(1000)) {
        override def extractTimestamp(t: (String, Double, Long)) = t._3.toLong
      }
    ).keyBy(_._1)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(500)))
      .reduce(
        (x, y) => {
          (x._1, x._2 + y._3, 0L)
        }
      ).map(_._2).print("windows:::::").setParallelism(1)
    env.execute(this.getClass.getSimpleName.stripSuffix("$"))
  }
}
