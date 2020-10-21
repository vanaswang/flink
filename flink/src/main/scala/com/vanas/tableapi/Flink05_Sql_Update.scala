package com.vanas.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

/**
 * @author Vanas
 * @create 2020-08-14 9:11 上午 
 */
object Flink05_Sql_Update {
    def main(args: Array[String]): Unit = {
        // 流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // 1. 读取数据，并封装成样例类
        val sensorDS = env.readTextFile("input/sensor.txt")
                .map(
                    data => {
                        val datas = data.split(",")
                        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
                    }
                )

        val settings = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        //2从datastream转换成table

        val sensorTable: Table = tableEnv.fromDataStream(sensorDS)

        //3.创建临时试图 给动态table指定一个表名
        tableEnv.createTemporaryView("sensor", sensorTable)


        //4.使用sql进行操作
        tableEnv.sqlQuery(
            "select * from sensor"
            //"select * from "+sensorTable //没给表名直接拼接上表对象不推荐
        )
                .toAppendStream[Row]
                .print()


        env.execute()

    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
