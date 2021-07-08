package com.rison.flink.sideoutput

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/7/8 下午4:23
 *         测输出流
 *         FreezingMonitor 函数，用来监控传感器温度值，将温度值低于
 *         32F 的温度输出到 side output
 */
object SideOutPutMain {
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
    dataMap.process(FreezingMonitor()).getSideOutput(new OutputTag[String]("freezing-alarms")).print()
    env.execute()
  }
}

case class FreezingMonitor() extends ProcessFunction[(String, Double, Long), (String, Double, Long)] {
  lazy val freezingAlarmOutPut: OutputTag[String] = new OutputTag[String]("freezing-alarms")

  override def processElement(in: (String, Double, Long),
                              context: ProcessFunction[(String, Double, Long), (String, Double, Long)]#Context,
                              collector: Collector[(String, Double, Long)]): Unit = {
    if (in._2 > 32.0) {
      context.output(freezingAlarmOutPut, s"freezing Alarm ${in._1}")
    }
    collector.collect(in)
  }
}