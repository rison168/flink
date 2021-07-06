package com.rison.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/7/6 下午4:40
 * 流处理的wordCount
 */
object StreamWordCount{
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //创建流出具环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //接收socket文本流
    val textDS: DataStream[String] = env.socketTextStream(host, port)
    val dataStream: DataStream[(String, Int)] = textDS.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)
    dataStream.print.setParallelism(1)
    env.execute("socket stream word count")

  }
}
