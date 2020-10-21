package com.vanas.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row

/**
 * @author Vanas
 * @create 2020-08-14 9:11 上午 
 */
object Flink03_Sql_TableAPI {
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
        //可以给列名去别名，用as，别名也用单引号
        //如果列名匹配不到，那么就根据位置去匹配
        //如果列名能匹配到，那么可以随便改位置
        //列名可以指定时间语义，直接.proctime或.rowtime
        //val table: Table = tableEnv.fromDataStream(sensorDS,'a,'b)
        val table: Table = tableEnv.fromDataStream(sensorDS, 'id, 'ts)


        //3保存Tbale
        //将外部系统，抽象成一张表
        //connect =>指定连接的外部系统
        //withFormat  指定外部系统的存储格式
        //withschema 外部系统抽象出来的表，他的数据结构。也就是字段名字段类型
        //将外部系统抽象出来的表创建出来，给定表名
        //connect 跟某外部系统进行连接，可以是文件、kafka、数据库
        tableEnv
                .connect(new FileSystem().path("output/sensor.csv"))
                .withFormat(new OldCsv().fieldDelimiter("|"))
                .withSchema(
                    new Schema()
                            .field("id", DataTypes.STRING())
                            .field("ts", DataTypes.BIGINT())
                )
                .createTemporaryTable("sensor")

        //补充：从外部系统读取
        //外部系统
        tableEnv.sqlQuery("select * from sensor").toAppendStream[Row].print()


        //通过对table的插入，实现对外部系统的保存
        table.insertInto("sensor")

        env.execute()

    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
