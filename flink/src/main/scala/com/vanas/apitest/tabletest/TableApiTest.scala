package com.vanas.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

/**
 * @author Vanas
 * @create 2020-08-10 11:44 下午 
 */
object TableApiTest {
    def main(args: Array[String]): Unit = {
        //1.创建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val tableEnv = StreamTableEnvironment.create(env)
        /*
        //1.1基于老版本的planner的流处理
        val settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build()
        val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

        //1.2基于老版本的批处理
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

        //1.3基于blink planner的流处理
        val blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build()
        val blinkStreamTableEnv = StreamTableEnvironment.create(env, settings)

        //1.4基于blink planner的批处理
        val blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build()
        val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
        */
        //2.连接外部系统，读取数据，注册表
        //2.1读取文件
        val filePath = "/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt"
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv)
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable")

        //2.2从kafka读取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("kafkaInputTable")

        //3.查询转换
        //3.1使用table api
        val sensorTable = tableEnv.from("inputTable")
        val resultTable = sensorTable
                .select('id, 'temperature)
                .filter('id === "sensor_1")

        //3.2SQL
        val resultSqlTable = tableEnv.sqlQuery(
            """
              |select id,temperature
              |from inputTable
              |where id = 'sensor_1'
              |""".stripMargin)


        //val inputTable: Table = tableEnv.from("kafkaInputTable")
        //inputTable.toAppendStream[(String, Long, Double)].print()


        resultTable.toAppendStream[(String,Double)].print("result")
        resultSqlTable.toAppendStream[(String,Double)].print("sql")

        env.execute("table api test")

    }

}
