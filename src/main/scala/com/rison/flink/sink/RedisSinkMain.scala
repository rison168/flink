package com.rison.flink.sink

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


/**
 * @author : Rison 2021/7/7 上午10:06
 *         Redis Sink
 */
object RedisSinkMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val redisConf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.1.215")
      .setPort(6379)
      .setPassword("xxxx")
      .setDatabase(12)
      .build()
    env.fromCollection(
      List(
        RedisDemo("key_1", "value_1"),
        RedisDemo("key_2", "value_2")
      )
    ).addSink(new RedisSink[RedisDemo](redisConf, MyRedisMapper()))
    env.addSource(MyRedisSource("redis:sink:instance")).print()
    env.execute("redis sink")
  }
}

case class RedisDemo(key: String, value: String)

case class MyRedisMapper() extends RedisMapper[RedisDemo] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "redis:sink:instance")
  }

  override def getKeyFromData(in: RedisDemo): String = {
    in.key
  }

  override def getValueFromData(in: RedisDemo): String = {
    in.value
  }
}

case class MyRedisSource(key: String) extends RichSourceFunction[RedisDemo] {
  var jedisPool: JedisPool = _
  var jedis: Jedis = _

  override def open(parameters: Configuration): Unit = {
    jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.1.215", 6379, 10000, "xxxx", 12, "myRedisClient")

  }

  override def run(sourceContext: SourceFunction.SourceContext[RedisDemo]): Unit = {
    import scala.collection.JavaConversions._
    jedis = jedisPool.getResource
    val hashMap: util.Map[String, String] = jedis.hgetAll(key)
    hashMap.toList.foreach(
      data => {
        sourceContext.collect(RedisDemo(data._1, data._2))
      }
    )


  }

  override def cancel(): Unit = {
    jedis.close()
    jedisPool.close()

  }
}
