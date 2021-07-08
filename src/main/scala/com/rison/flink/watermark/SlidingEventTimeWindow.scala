package com.rison.flink.watermark

import javafx.util.converter.TimeStringConverter
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

/**
 * @author : Rison 2021/7/8 下午2:21
 *         滑动窗口
 *
 */
object SlidingEventTimeWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val textWithEventTimeDataStream: DataStream[(String, Double, Long)] = dataStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        (arr(0).toString, arr(1).toDouble, arr(2).toLong)
      }
    ).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Double, Long)](Time.milliseconds(1000)) {
        override def extractTimestamp(t: (String, Double, Long)) = {
          t._3
        }
      }
    )
    val windowDataStream: WindowedStream[(String, Double, Long), String, TimeWindow] = textWithEventTimeDataStream
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.milliseconds(500)))
    windowDataStream.fold(new mutable.HashSet[Long]()) {
      case (set, (key, temp, ts)) => set += ts
    }
    env.execute(this.getClass.getSimpleName.stripSuffix("$"))
  }
}
