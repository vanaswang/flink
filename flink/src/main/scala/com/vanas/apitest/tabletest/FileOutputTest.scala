package com.vanas.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @author Vanas
 * @create 2020-08-11 12:59 上午 
 */
object FileOutputTest {
    def main(args: Array[String]): Unit = {
        // 1. 创建环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val tableEnv = StreamTableEnvironment.create(env)

        // 2. 连接外部系统，读取数据，注册表
        val filePath = "/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt"

        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable")

        // 3. 转换操作
        val sensorTable = tableEnv.from("inputTable")
        // 3.1 简单转换
        val resultTable = sensorTable
                .select('id, 'temp)
                .filter('id === "sensor_1")

        // 3.2 聚合转换
        val aggTable = sensorTable
                .groupBy('id)    // 基于id分组
                .select('id, 'id.count as 'count)

        // 4. 输出到文件
        // 注册输出表
        val outputPath = "/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt"

        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE())
                    //        .field("cnt", DataTypes.BIGINT())
                )
                .createTemporaryTable("outputTable")

        resultTable.insertInto("outputTable")

        //    resultTable.toAppendStream[(String, Double)].print("result")
        //    aggTable.toRetractStream[(String, Long)].print("agg")

        env.execute()
    }

}
