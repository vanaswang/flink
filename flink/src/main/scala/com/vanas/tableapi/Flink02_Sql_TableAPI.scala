package com.vanas.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

/**
 * @author Vanas
 * @create 2020-08-14 9:11 上午 
 */
object Flink02_Sql_TableAPI {
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
        //可以给列名去别名，用as，别名也用单引号
        //如果列名匹配不到，那么就根据位置去匹配
        //如果列名能匹配到，那么可以随便改位置
        //列名可以指定时间语义，直接.proctime或.rowtime
        //val table: Table = tableEnv.fromDataStream(sensorDS,'a,'b)
        val table: Table = tableEnv.fromDataStream(sensorDS, 'id, 'ts)


        //使用table api
        val resultTable = table
                .groupBy('id)
                .select('id, 'id.count)
                //.toAppendStream[Row] //table is not an append only table

                //如果需要更新数据，要用retract stream
                .toRetractStream[Row]
                //.toRetractStream[(Boolean, String, Integer)]
                .print("table api")

        //将table转换成DataStream
        //toAppendStream    ：转换成追加流


        env.execute()

    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
