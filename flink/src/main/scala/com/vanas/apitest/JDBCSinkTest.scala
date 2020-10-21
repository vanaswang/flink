package com.vanas.apitest

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author Vanas
 * @create 2020-08-07 5:34 下午 
 */
object JDBCSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt")

        val stream: DataStream[SensorReading] = env.addSource(new MySensorSource())

        val dataStream = inputStream.map(data => {
            val arr: Array[String] = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })
        //dataStream.addSink(new MyJdbcSinkFun())
        stream.addSink(new MyJdbcSinkFun())
        env.execute("jdbc sink test")
    }
}

class MyJdbcSinkFun() extends RichSinkFunction[SensorReading] {
    //定义连接、预编译语句
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updataStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "0509")
        insertStmt = conn.prepareStatement("insert into sensor_temp(id,temp) val (?,?) ")
        updataStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
    }

    override def invoke(value: SensorReading): Unit = {
        //先执行更新操作，查到就更新
        updataStmt.setDouble(1, value.temperature)
        updataStmt.setString(2, value.id)
        updataStmt.execute()

        //如果更新没有查到数据，那么就插入
        if (updataStmt.getUpdateCount == 0) {
            insertStmt.setString(1, value.id)
            insertStmt.setDouble(2, value.temperature)
            insertStmt.execute()
        }
    }

    override def close(): Unit = {
        insertStmt.close()
        updataStmt.close()
        conn.close()
    }
}
