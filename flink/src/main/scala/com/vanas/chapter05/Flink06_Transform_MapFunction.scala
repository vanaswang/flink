package com.vanas.chapter05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author Vanas
 * @create 2020-08-05 10:28 上午 
 */
object Flink06_Transform_MapFunction {
    def main(args: Array[String]): Unit = {

        //val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //流式处理环境
        //        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        val lineDS = env.readTextFile("/")
        lineDS.map(
            line => {
                val datas: Array[String] = line.split(",")
            }
        )


        env.execute()

    }

    //自定义SourceFunction
    //1.实现（混入—）MapFunction,指定范型
    //2.重写run和cancel方法
    class MyMapFunction extends MapFunction[String, WaterSensor] {
        //@volatile 类型修饰符，内存可见性，Java
        override def map(line: String): WaterSensor = {
            val datas: Array[String] = line.split(",")
            WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)

        }
    }

    //定义样例类：水位传感器，用于接收空高数据
    //id：传感器编号
    //ts：时间戳
    //vc：空高
    case class WaterSensor(id: String, ts: Long, vc: Int)

}
