package com.vanas.apitest

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * @author Vanas
 * @create 2020-08-05 8:08 下午 
 */
object TransfromTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //0.读取数据
        val inputPath = "/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt"
        val inputStream: DataStream[String] = env.readTextFile(inputPath)

        //1。先转换成样例类类型(简单转换操作）
        val dataStream = inputStream
                .map(data => {
                    val arr = data.split(",")
                    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
                }).filter(new MyFilter)
        //2.分组聚合，输出每个传感器温度最小值
        val aggStream = dataStream
                .keyBy("id") //以id进行分组
                //.min("temperature") //只是做了最小值的提取
                .minBy("temperature") //拿到一整条数据
        //3.需要输出当前最小的温度值，以及最近的时间戳，要用reduce
        val resultStram = dataStream
                .keyBy("id")
                //.reduce((curState, newData) =>
                //   SensorReading(curState.id, newData.timestramp, curState.timestramp.min(newData.temperature))
                //)
                .reduce(new MyReduceFunction)
        //resultStram.print()

        //4.多留转换操作
        //4。1分流，将传感器温度数据分成低温】高温两条流
        val splitStream: SplitStream[SensorReading] = dataStream.split(
            data => {
                if (data.temperature > 30.0) Seq("high") else Seq("low")
            })
        val highTempStream: DataStream[SensorReading] = splitStream.select("high")
        val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
        val allTempSteam: DataStream[SensorReading] = splitStream.select("high", "low")

        //highTempStream.print("high")
        //lowTempSteam.print("low")
        //allTempSteam.print("all")

        //4.2合流 connect
        val warningStream = highTempStream.map(data => (data.id, data.temperature))
        val connectedStreams = warningStream.connect(lowTempStream)

        //用coMap对数据进行分别处理
        val coMapResultStream: DataStream[Product] = connectedStreams
                .map(
                    warningData => (warningData._1, warningData._2, "warning"),
                    lowTempStream => (lowTempStream.id, "healthy")
                )
        coMapResultStream.print()

        //4.3union 合流 //如果是warningStream 会报错因为它是二元组，不匹配
        val unionStream = highTempStream.union(lowTempStream, allTempSteam)

        env.execute("transfrom test")
    }
}

class MyReduceFunction extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        SensorReading(value1.id, value2.timestramp, value1.temperature.min(value2.temperature))
    }
}

//自定义一个函数类
class MyFilter extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = {
        value.id.startsWith("sensor_1")
    }
}

class MyMapper extends MapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = value.id + "temperature"
}

//富函数，可以获取运行时上下文，还有一些生命周期
class MyRichMapper extends RichMapFunction[SensorReading, String] {


    override def open(parameters: Configuration): Unit = {
        //做一些初始化操作，比如数据库连接
        getRuntimeContext()
    }

    override def map(value: SensorReading): String = value.id + "temperature"

    override def close(): Unit = {
        //一般做收尾工作，比如关闭连接，或清空状态
    }

}