package com.rison.flink.watermark

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author : Rison 2021/7/8 上午10:21
 *         Watermark
 */
object WaterMarkMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 8888)
    dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(1000)) {
      override def extractTimestamp(t: String) = t.toLong * 1000
    })

    env.execute("water mark")

  }
}
