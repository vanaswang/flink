package com.vanas.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Schema}

/**
 * @author Vanas
 * @create 2020-08-11 2:20 下午 
 */
object EsOutputTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


        val filePath = "/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt"
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable")


        //3.查询转换
        //3.1简单转换
        val sensorTable = tableEnv.from("inputTable")
        val resultTable = sensorTable
                .select('id, 'temperature)
                .filter('id === "sensor_1")

        // 3.2 聚合转换
        val aggTable = sensorTable
                .groupBy('id) // 基于id分组
                .select('id, 'id.count as 'count)


        //4.输出到es
        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("localhost", 9200, "http")
                .index("sensor")
                .documentType("temperature")
        )
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count", DataTypes.BIGINT())

                )
                .createTemporaryTable("esOutputTable")
        aggTable.insertInto("esOutputTable")
        env.execute("es output test")


    }

}
