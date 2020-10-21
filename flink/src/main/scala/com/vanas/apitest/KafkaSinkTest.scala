package com.vanas.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @author Vanas
 * @create 2020-08-07 3:08 下午 
 */
object KafkaSinkTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        //读取数据
        val inputStream = env.readTextFile("/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt")
        //从kafka读数据

        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop103:9092")
        properties.setProperty("group.id", "consumer-group")
        val stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

        //val dataStream = inputStream
        val dataStream = stream
                .map(data => {
            val arr: Array[String] = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
        })


        dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092", "sinktest", new SimpleStringSchema()))
        env.execute("kafka sink")

    }

}
