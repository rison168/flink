package com.rison.flink

import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/7/6 下午6:27
 *         转换算子
 */
object TransFormFunction {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = env.readTextFile("data/hello.txt")
    //map
    dataStream.map(
      data => {
        data.toInt * 2
      }
    )

    //flatMap
    /**
     * flatMap 的函数签名：def flatMap[A,B](as: List[A])(f: A ⇒ List[B]): List[B]
     * 例如: flatMap(List(1,2,3))(i ⇒ List(i,i))
     * 结果是 List(1,1,2,2,3,3), 而 List("a b", "c d").flatMap(line ⇒ line.split(" "))
     * 结果是 List(a, b, c, d)。
     */
    dataStream.flatMap(
      data => {
        data.split(" ")
      }
    )

    // Filter
    dataStream.filter(
      data => {
        data.toInt == 1
      }
    )

    //keyBy
    /**
     * DataStream → KeyedStream：逻辑地将一个流拆分成不相交的分区，每个分
     * 区包含具有相同 key 的元素，在内部以 hash 的形式实现的。
     */


    //滚动聚合算子 Rolling Aggregation
    /**
     * 这些算子可以针对 KeyedStream 的每一个支流做聚合。
     *  sum()
     *  min()
     *  max()
     *  minBy()
     *  maxBy()
     */

    // reduce
    /**
     * KeyedStream → DataStream：一个分组数据流的聚合操作，合并当前的元素
     * 和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是
     * 只返回最后一次聚合的最终结果
     */

    dataStream.map(
      data => {
        val strings: Array[String] = data.split(",")
        (data(0).toLong, data(1).toInt, data(1).toInt)
      }
    )
      .keyBy(0)
      .reduce(
        (x, y) => {
          (x._1, x._2, x._3 + y._3)
        }
      )

    //Split 和 Select
    /**
     * Split :
     * DataStream → SplitStream：根据某些特征把一个 DataStream 拆分成两个或者
     * 多个 DataStream。
     *
     * Select ：
     * SplitStream→DataStream：从一个 SplitStream 中获取一个或者多个
     * DataStream。
     */

    val splitDataStream: SplitStream[(Long, Int)] = dataStream
      .map(
        data => {
          val strings: Array[String] = data.split(",")
          (strings(0).toLong, strings(1).toInt)
        }
      )
      .split(
        data => {
          if (data._2 > 30) Seq("high") else Seq("low")
        }
      )

    val high: DataStream[(Long, Int)] = splitDataStream.select("high")
    val low: DataStream[(Long, Int)] = splitDataStream.select("low")
    val all: DataStream[(Long, Int)] = splitDataStream.select("high", "low")

    // Connect 和 CoMap

    /**
     * DataStream,DataStream → ConnectedStreams：连接两个保持他们类型的数
     * 据流，两个数据流被 Connect 之后，只是被放在了一个同一个流中，内部依然保持
     * 各自的数据和形式不发生任何变化，两个流相互独立
     *
     * ConnectedStreams → DataStream：作用于 ConnectedStreams 上，功能与 map
     * 和 flatMap 一样，对 ConnectedStreams 中的每一个 Stream 分别进行 map 和 flatMap
     * 处理
     */

    val connectDataStream: ConnectedStreams[(Long, Int), (Long, Int)] = high.connect(low)

    val coMap: DataStream[((Long, Int), String)] = connectDataStream.map(
      highData => (highData, "high"),
      lowData => (lowData, "low")
    )

    //union
    /**
     * DataStream → DataStream：对两个或者两个以上的 DataStream 进行 union 操
     * 作，产生一个包含所有 DataStream 元素的新 DataStream
     *
     * 1． Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap
     * 中再去调整成为一样的。
     * 2. Connect 只能操作两个流，Union 可以操作多个
     */
    val union: DataStream[(Long, Int)] = high.union(low)


  }
}
