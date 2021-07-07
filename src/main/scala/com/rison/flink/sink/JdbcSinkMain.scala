package com.rison.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author : Rison 2021/7/7 上午11:20
 *         JDBC Sink
 */
object JdbcSinkMain {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(
      List(
        MySqlDemo("lisi", 25),
        MySqlDemo("wangwu", 23)
      )
    )
      .addSink(MySqlSink())
    env.execute("mysql sink instance")
  }
}

case class MySqlDemo(name: String, age: Int)

case class MySqlSink() extends RichSinkFunction[MySqlDemo] {
  var conn: Connection = _
  var insert_stmt: PreparedStatement = _
  var update_stmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql//localhost:3306/test", "root", "123456")
    insert_stmt = conn.prepareStatement(
      """
        |insert into user(name, age) values (?,?)
        |""".stripMargin
    )
    update_stmt = conn.prepareStatement(
      """
        |update user
        |set age = ?
        |where name = ?
        |""".stripMargin
    )


  }

  override def invoke(value: MySqlDemo, context: SinkFunction.Context[_]): Unit = {
    update_stmt.setInt(1, value.age)
    update_stmt.setString(2, value.name)
    update_stmt.execute()
    if (update_stmt.getUpdateCount == 0) {
      insert_stmt.setInt(2, value.age)
      insert_stmt.setString(1, value.name)
      insert_stmt.execute()
    }

  }

  override def close(): Unit = {
    update_stmt.close()
    insert_stmt.close()
    conn.close()
  }

}