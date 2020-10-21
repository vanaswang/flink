package com.vanas.chapter05

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @author Vanas
 * @create 2020-08-05 10:28 上午 
 */
object Flink03_Source_Kafka {
    def main(args: Array[String]): Unit = {

        //val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //流式处理环境
//        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        //从Kafka中读取数据
        val topic = "sensor"
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop102:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        val kafkaDS: DataStream[String] = env.addSource(
            new FlinkKafkaConsumer011[String](
                topic,
                new SimpleStringSchema(),
                properties
            )
        )

        kafkaDS.print()
        env.execute()

    }

}
