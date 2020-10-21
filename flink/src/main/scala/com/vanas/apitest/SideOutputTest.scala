package com.vanas.apitest

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author Vanas
 * @create 2020-08-08 10:58 下午 
 */
object SideOutputTest {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStateBackend(new FsStateBackend(""))
        val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

        val dataStream: DataStream[SensorReading] = inputStream.map(
            data => {
                val arr: Array[String] = data.split(",")
                SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
            })
        val highTempStream = dataStream
                .process(new SplitTempProcessor(30.0))

        highTempStream.print("high")
        highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")

        env.execute()
    }
}

//实现自定义ProcessFunction进行分流
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        if (value.temperature > threshold) {
            //如果当前温度大于30，那么输出到主流
            out.collect(value)
        } else {
            //如果不超过30度，那么输出到侧输出流
            ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestramp, value.temperature))
        }
    }
}
