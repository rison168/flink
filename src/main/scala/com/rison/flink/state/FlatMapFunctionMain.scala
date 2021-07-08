package com.rison.flink.state

import com.rison.flink.bean.Sensor
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/7/8 下午8:46
 *
 */
object FlatMapFunctionMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream: DataStream[Sensor] = env.socketTextStream("localhost", 7777)
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          Sensor(arr(0).toString, arr(1).toDouble, arr(2).toLong)
        }
      )
    dataStream.keyBy(_.id)
      .flatMap(TemperatureAlertFunction(1.7))


    env.execute()
  }
}

case class TemperatureAlertFunction(threshold: Double) extends RichFlatMapFunction[Sensor, Sensor]{
  lazy private val lastTempState = getIterationRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))


  override def flatMap(in: Sensor, collector: Collector[Sensor]): Unit = {
    val lastTemp: Double = lastTempState.value()
    val tempAbs: Double = (in.temperature - lastTemp).abs
    if (tempAbs > threshold){
      collector.collect(in)
    }
    this.lastTempState.update(in.temperature)

  }
}
