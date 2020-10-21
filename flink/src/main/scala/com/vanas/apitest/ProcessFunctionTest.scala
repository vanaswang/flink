package com.vanas.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author Vanas
 * @create 2020-08-08 9:10 下午 
 */
object ProcessFunctionTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

        val dataStream = inputStream.map(data => {
            val arr: Array[String] = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })
        //.keyBy(_.id)
        //.process(new MyKeyedProcessFunction)
        val warningStream: DataStream[String] = dataStream
                .keyBy(_.id)
                .process(new TempIncreWarning(10000L))
        warningStream.print()


        env.execute("process function test")


    }

}

//实现自定义的KeyedProcessFunction
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {
    //定义状态：保存上一个温度值进行比较，保存注册时间器的时间戳用于删除
    lazy val lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    lazy val timerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))


    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
        //先取出状态
        val lastTemp = lastTempState.value()
        val timerTs = timerTsState.value()

        //更新温度值
        lastTempState.update(value.temperature)

        //判断当前温度值和上次温度进行比较
        if (value.temperature > lastTemp && timerTs == 0) {
            //如果温度上升，且没有定时器，那么注册当前时间10s之后的定时器
            val ts: Long = ctx.timerService().currentProcessingTime() + interval
            ctx.timerService().registerProcessingTimeTimer(ts)
            timerTsState.update(ts)
        } else if (value.temperature < lastTemp) {
            //如果温度下降，那么删除定时器
            ctx.timerService().deleteProcessingTimeTimer(timerTs)
            timerTsState.clear()
        }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval / 1000 + "秒连续上升")
        timerTsState.clear()
    }
}

//KeyedProcessFunction 功能测试
class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {
    var myState: ValueState[Int] = _

    override def open(parameters: Configuration): Unit = {
        myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("mystate", classOf[Int]))
    }

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
        ctx.getCurrentKey
        ctx.timestamp()
        ctx.timerService().currentWatermark()
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L)
        //ctx.timerService().deleteEventTimeTimer()

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

    }

}
