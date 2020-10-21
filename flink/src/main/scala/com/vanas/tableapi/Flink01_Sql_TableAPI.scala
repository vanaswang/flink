package com.vanas.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}

/**
 * @author Vanas
 * @create 2020-08-14 9:11 上午 
 */
object Flink01_Sql_TableAPI {
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
        //从datastream转换成table
        val table: Table = tableEnv.fromDataStream(sensorDS)
        //使用table api算子
        val resultTable: Table = table
                .select('id, 'ts, 'vc)
                .where('id === "sensorTable")

        //将table转换成DataStream
        //toAppendStream    ：转换成追加流
        resultTable.toAppendStream[WaterSensor].print("table api")

        env.execute()

    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
