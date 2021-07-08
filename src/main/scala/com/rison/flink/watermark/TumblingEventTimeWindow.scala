package com.rison.flink.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

/**
 * @author : Rison 2021/7/8 上午11:31
 *         滚动窗口
 */
object TumblingEventTimeWindow {
  def main(args: Array[String]): Unit = {
    //环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val textDS: DataStream[(String, Double, Long)] = dataStream
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          (arr(0).toString, arr(1).toDouble, arr(2).toLong)
        }
      )
    val textWithEventTimeDataStream: DataStream[(String, Double, Long)] = textDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Double, Long)](Time.milliseconds(1000)) {
        override def extractTimestamp(in: (String, Double, Long)) = {
          in._3
        }
      })

    textWithEventTimeDataStream.keyBy(0).print("key:")

    val windowStream: WindowedStream[(String, Double, Long), Tuple, TimeWindow] = textWithEventTimeDataStream
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))

    val groupDStream: DataStream[mutable.HashSet[Long]] = windowStream.fold(new mutable.HashSet[Long]()) {
      case (set, (key, temp, ts)) => set += ts
    }
    groupDStream.print("window:::::").setParallelism(1)
    env.execute(this.getClass.getSimpleName.stripSuffix("$"))
  }
}
