package com.rison.flink.sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @author : Rison 2021/7/7 上午11:01
 *         elasticSearch sink
 */
object ElasticSearchSinkMain {
  def main(args: Array[String]): Unit = {
    val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("localhost", 9200))
    val esSinkBuilder: ElasticsearchSink[ElasticSearchDemo] = new ElasticsearchSink.Builder[ElasticSearchDemo](hosts,
      new ElasticsearchSinkFunction[ElasticSearchDemo] {
        override def process(in: ElasticSearchDemo, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
          val indexRequest: IndexRequest = Requests
            .indexRequest()
            .index(in.index)
            .`type`(in.`type`)
            .source(in.toString)
          requestIndexer.add(indexRequest)
        }
      }).build()

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(
      List(
        ElasticSearchDemo("index_1", "_doc", "value_1"),
        ElasticSearchDemo("index_2", "_doc", "value_1")
      )
    )
      .addSink(esSinkBuilder)
    env.execute("elasticSearch sink")
  }
}

case class ElasticSearchDemo(index: String, `type`: String, value: String)
