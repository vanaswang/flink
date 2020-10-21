package com.vanas.chapter05

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * @author Vanas
 * @create 2020-08-05 10:28 上午 
 */
object Flink05_Transform_Map {
    def main(args: Array[String]): Unit = {

        //val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //流式处理环境
        //        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        val lineDS = env.readTextFile("/")
        lineDS.map(
            line=>{
                val datas: Array[String] = line.split(",")
//                WaterSensor(datas(0), datas(1).toLong, datas(2).toDouble)
            }
        )


        env.execute()

    }

    //自定义SourceFunction
    //1.实现（混入—）SourceFunction,指定范型
    //2.重写run和cancel方法
    class MySourceFunction extends SourceFunction[WaterSensor] {
        //@volatile 类型修饰符，内存可见性，Java
        @volatile var flag: Boolean = true

        override def run(sourceContext: SourceFunction.SourceContext[WaterSensor]): Unit = {
            while (flag) {
                val sensor: WaterSensor = WaterSensor("sensor_" + Random.nextInt(3), System.currentTimeMillis(), 40 + Random.nextInt(10))
                sourceContext.collect(sensor)
                Thread.sleep(1000L)
            }
        }

        override def cancel(): Unit = {
            flag = false
        }
    }

    //定义样例类：水位传感器，用于接收空高数据
    //id：传感器编号
    //ts：时间戳
    //vc：空高
    case class WaterSensor(id: String, ts: Long, vc: Int)

}
