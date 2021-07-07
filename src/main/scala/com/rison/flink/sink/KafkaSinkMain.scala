package com.rison.flink.sink

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * @author : Rison 2021/7/7 上午9:49
 *         kafka Sink
 */
object KafkaSinkMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(
      List(
        KafkaDemo("kafka_1", "topic_1"),
        KafkaDemo("kafka_2", "topic_2")
      )
    )
      .map(
        data => {
          data.toString
        }
      )
      .addSink(new FlinkKafkaProducer011[String]("localhost:9092", "test", new SimpleStringSchema()))
    env.execute("kafka sink main")

  }
}

case class KafkaDemo(key: String, topic: String)
