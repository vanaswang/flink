package com.vanas.apitest

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * @author Vanas
 * @create 2020-08-07 2:36 下午 
 */
object FileSinkTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //读取数据
        val inputPath = "/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath)
        //转换成样例类类型（简单转换操作）
        val dataStream: DataStream[SensorReading] = inputStream
                .map(data => {
                    val arr: Array[String] = data.split(",")
                    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
                })
        dataStream.print()
        //dataStream.writeAsCsv("/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/out.txt")
        dataStream.addSink(StreamingFileSink.forRowFormat(
            new Path("/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt"),
            new SimpleStringEncoder[SensorReading]()
        ).build())


        env.execute("file sink test")
    }

}
