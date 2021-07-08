package com.rison.flink.process

import com.rison.flink.bean.Sensor
import org.apache.avro.Schema.Type
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author : Rison 2021/7/8 下午3:41
 *         监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连
 *         续上升，则报警。
 */
object KeyedProcessFunctionMain {
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
      .process(TempIncreaseAlertFunction())

    env.execute()
  }
}

case class TempIncreaseAlertFunction() extends KeyedProcessFunction[String, Sensor, String] {
  //保存上一个传感器温度值
  lazy val lastTempState: ValueState[Double] = getIterationRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", Types.of[Double])
  )
  //保存注册的定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getIterationRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer", Types.of[Long])
  )

  //保存注册的定时器的时间戳
  override def processElement(in: Sensor, context: KeyedProcessFunction[String, Sensor, String]#Context, collector: Collector[String]): Unit = {
    //取出上一次的温度
    val preTemp: Double = lastTempState.value()
    //当前温度更新到上一个温度这个变量中
    lastTempState.update(in.temperature)
    val currentTimeStamp: Long = currentTimer.value()
    if (preTemp == 0.0 || in.temperature < preTemp) {
      //温度下降或者第一个温度值，删除定时器
      context.timerService().deleteProcessingTimeTimer(currentTimeStamp)
      //清除状态的变量
      currentTimer.clear()
    }
    else if (in.temperature > preTemp && currentTimeStamp == 0) {
      //温度上升且还没设置定时器
      val timerTs: Long = context.timerService().currentProcessingTime() + 1000
      context.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器id为：" + ctx.getCurrentKey + "的传感器温度值已经1s上升了。")
    currentTimer.clear()
  }
}
