package com.rison.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @author : Rison 2021/7/6 下午5:50
 *         不同的方读取数据
 */
object OriginDataType {

  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //从集合读取数据
  private val dataStream1: DataStream[SensorReading] = env.fromCollection(
    List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )
  )

  //从文件读取数据
  private val dataStream2: DataStream[String] = env.readTextFile("/data")

  //从kafka读取数据
  /**
   * 引入jar
   * <dependency>
   * <groupId>org.apache.flink</groupId>
   * <artifactId>flink-connector-kafka-0.11_2.12</artifactId>
   * <version>1.10.1</version>
   * </dependency>
   */

  private val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9020")
  properties.setProperty("group.id", "consumer_group")
  properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("auto.offset.reset", "latest")
  private val dataStream3: DataStream[String] = env.addSource(
    new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties)
  )

  //自定义source
  private val dataStream4: DataStream[SensorReading] = env.addSource(
    MySenSorSource()
  )


}

case class SensorReading(id: String, timestamp: Long, temperature: Double)

case class MySenSorSource() extends SourceFunction[SensorReading] {
  // flag：表示数据源是否还在正常运行
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始话一个随机数发生器
    val random = new Random()
    var tuples = 1.to(10).map(
      i => {
        ("sensor_" + i, 65 + random.nextGaussian() * 20)
      }
    )

    while (running) {
      //更新温度值
      tuples = tuples.map(
        t => (t._1, t._2 + random.nextGaussian())
      )

      //当前时间
      val curTime = System.currentTimeMillis()

      tuples.foreach(
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
