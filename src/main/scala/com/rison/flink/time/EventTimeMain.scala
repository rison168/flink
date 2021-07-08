package com.rison.flink.time

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/7/8 上午8:39
 *   EventTime
 */
object EventTimeMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //从调用时刻开始给env创建每一个stream追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //TODO 业务
    env.execute("event time")
  }
}
