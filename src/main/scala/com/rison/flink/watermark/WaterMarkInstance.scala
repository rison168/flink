package com.rison.flink.watermark

import com.rison.flink.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, TimestampAssigner}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author : Rison 2021/7/8 上午10:33
 *
 */
object WaterMarkInstance {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //从调用时刻开始给env创建每一个stream的追加时间特性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //每隔5秒产生一个watermark
    env.getConfig.setAutoWatermarkInterval(5000)
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    //没有乱序的情况下
    val withTimesAndWatermarks: DataStream[String] = dataStream.assignAscendingTimestamps(_.toLong)
    //乱序情况下
    dataStream.assignTimestampsAndWatermarks(MyAssigner())
//    dataStream.assignTimestampsAndWatermarks(PunctuatedAssigner())
    env.execute("water mark instance")
}

}
case class MyAssigner() extends AssignerWithPeriodicWatermarks[String]{
  val bound: Long = 60 * 1000 //延时为1分钟
  var maxTimestamp: Long = Long.MinValue //观察到最大的时间戳

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimestamp - bound)
  }

  override def extractTimestamp(t: String, l: Long): Long = {
    maxTimestamp = maxTimestamp.max(t.toLong)
    t.toLong
  }
}

case class PunctuatedAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val bound: Long = 60 * 1000
  override def checkAndGetNextWatermark(t: SensorReading, extractedTS: Long): Watermark = {
    if (t.id == "sensor_1"){
      new Watermark(extractedTS - bound)
    }else{
      null
    }
  }
  override def extractTimestamp(t: SensorReading, extractedTS: Long): Long = {
    t.timestamp
  }
}