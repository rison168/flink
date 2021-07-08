package com.rison.flink.state

import com.rison.flink.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/7/8 下午9:00
 * state 实现FlatMapFunction
 */
object FlatMapFunctionStateMain {
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
      .flatMapWithState[Sensor, Double]{
        case (in: Sensor, None) => (List.empty,Some(in.temperature))
        case (in: Sensor, lastTemp:Some[Double]) => {
          val abs: Double = (in.temperature - lastTemp.get).abs
          if (abs > 1.7){
            (List(in), Some(in.temperature))
          }else{
            (List.empty, Some(in.temperature))
          }
        }
      }


    env.execute()
  }
}
