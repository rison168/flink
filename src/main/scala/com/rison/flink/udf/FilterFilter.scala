package com.rison.flink.udf

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/7/6 下午7:32
 *
 */
class FilterFilter extends FilterFunction[String]{
  override def filter(t: String): Boolean = {
    t.contains("flink")
  }
}

object FilterMain{
  def main(args: Array[String]): Unit = {
     val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = env.fromCollection(
      List("spark", "flink")
    )
    //外部函数定义
    dataStream.filter(new FilterFilter).print("外部函数：")

    //匿名函数
    dataStream.filter(
      new FilterFunction[String] {
        override def filter(t: String) = {
          t.contains("spark")
        }
      }
    ).print("匿名函数：")

    //带参判断

    dataStream.filter(
      FilterKey("flink")
    ).print("带参数flink:")

    env.execute("Filter main")
  }
}

case class FilterKey(key: String) extends FilterFunction[String]{
  override def filter(t: String): Boolean = {
    t.contains(key)
  }
}