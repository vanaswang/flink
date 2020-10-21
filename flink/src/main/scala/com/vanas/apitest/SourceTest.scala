package com.vanas.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random

/**
 * @author Vanas
 * @create 2020-08-05 6:50 下午
 */

//定义样例类，温度传感器
case class SensorReading(id: String, timestramp: Long, temperature: Double)

object SourceTest {
    def main(args: Array[String]): Unit = {
        //创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


        //1.从集合中读取数据
        val dataList = List(
            SensorReading("sensor_1", 1547718199, 35.8),
            SensorReading("sensor_6", 1547718201, 15.4),
            SensorReading("sensor_7", 1547718202, 6.7),
            SensorReading("sensor_10", 1547718205, 38.1)
        )
        val stream1 = env.fromCollection(dataList)
        //相当于大招里面什么类型都能放
        //env.fromElements(1.0,35,"hello")
        stream1.print()

        //2.从文件读取数据
        val inputPath = "/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt"
        val stream2: DataStream[String] = env.readTextFile(inputPath)
        stream2.print()

        //3.从kafka中读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "consumer-group")
        val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
        stream3.print()


        //4.自定义Source
        val stream4 = env.addSource(new MySensorSource())
        stream4.print()
        //执行
        env.execute("source test")

    }
}

//自定义sourceFunction
class MySensorSource() extends SourceFunction[SensorReading] {
    //定义一个标志位flag，用来数据愿是否正常发出数据
    var running = true

    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
        //定义一个随机数发生器
        val rand = new Random()
        //随机生产一组（10个)传感器的初始温度：（id，temp）
        val curTmp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))
        //定义无限循环，不停的产生数据，除非被cancel
        while (running) {
            //在上次的温度上微调，更新温度值
            val curTemp: immutable.IndexedSeq[(String, Double)] = curTmp.map(
                data => (data._1, data._2 + rand.nextGaussian())
            )
            //获取当前时间戳，加入到数据中，调用sourceContext.collect发出数据
            val curTime: Long = System.currentTimeMillis()
            curTemp.foreach(
                data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
            )
            //间隔500ms
            Thread.sleep(500)

        }
    }

    override def cancel(): Unit = running = false
}
