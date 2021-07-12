package com.rison.flink.table

import com.rison.flink.bean.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.PlannerExpressionParserImpl.over

/**
 * @author : Rison 2021/7/12 下午9:24
 *
 */
object TableAPIWindowMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = env.readTextFile("data/sensor.txt")
    val dataStream: DataStream[Sensor] = inputStream.map(
      data => {
        val dataArray: Array[String] = data.split(" ")
        Sensor(data(0).toString, data(1).toDouble, data(2).toLong)
      }
    )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(1)) {
        override def extractTimestamp(t: Sensor) = t.timestamp * 1000L
      })
    //基于env创建tableEnv
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    // 从一条流创建一张表，按照字段去定义，并指定事件时间的时间字段
    val dataTable: Table = tableEnv.fromDataStream(dataStream, 'id,
      'temperature, 'ts.rowtime)
    //按照时间开窗聚合统计
    val resultTable: Table = dataTable
      .window(Tumble over 10.second on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count)

    val selectedStream: DataStream[(Boolean, (String, Long))] = resultTable
      .toRetractStream[(String, Long)]

    env.execute(this.getClass.getSimpleName.stripSuffix("$"))
  }
}
