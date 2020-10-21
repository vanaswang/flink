package com.vanas.app

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 指定时间范围内网站总浏览量（PV）的统计
 *
 * @author Vanas
 * @create 2020-08-12 2:29 下午 
 */
object Flink27_Case_PV_WindowAnalysis {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        //读取数据，并封装成样例类
        val userBehaviorDS: DataStream[UserBehavior] = env
                .readTextFile("/Users/vanas/Desktop/vanas/flink0213/input/UserBehavior.csv")
                .map(
                    line => {
                        val datas: Array[String] = line.split(",")
                        UserBehavior(
                            datas(0).toLong,
                            datas(1).toLong,
                            datas(2).toInt,
                            datas(3),
                            datas(4).toLong
                        )
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 10000)
        //2。过滤出pv的行为
        val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")
        val pv20neDS: DataStream[(String, Long)] = filterDS.map(data => ("pv", 1L))

        //3.按照pv进行分组
        val pv20neKS: KeyedStream[(String, Long), String] = pv20neDS.keyBy(_._1)
        pv20neKS
                .timeWindow(Time.hours(1))
                .sum(1)
                .print("pv")

        env.execute()

    }


    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}
