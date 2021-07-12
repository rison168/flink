package com.rison.flink.table

import com.rison.flink.bean.Sensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

/**
 * @author : Rison 2021/7/12 下午8:42
 *
 */
object TableAPIMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.readTextFile("data/sensor.txt")
    inputStream.print()
    val dataStream: DataStream[Sensor] = inputStream.map(
      data => {
        val dataArray: Array[String] = data.split(" ")
        Sensor(data(0).toString, data(1).toDouble, data(2).toLong)
      }
    )
    dataStream.print()

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //从一条流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)
    dataTable.printSchema()
    //从表里选取特定的数据
    val selectTable: Table = dataTable.select('id, 'temperature).filter("id = 'sensor_1'")
    val selectStream: DataStream[(String, Double)] = selectTable.toAppendStream[(String, Double)]
    selectStream.print()

    env.execute(this.getClass.getSimpleName.stripSuffix("$"))
  }
}
