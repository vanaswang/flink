package com.vanas.app

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author Vanas
 * @create 2020-08-12 2:45 下午 
 */
object Flink28_Case_UV_WindowAnalysis {
    def main(args: Array[String]): Unit = {
        // 流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 1. 读取数据，并封装成样例类
        val userBehaviorDS: DataStream[UserBehavior] = env
                .readTextFile("input/UserBehavior.csv")
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
                .assignAscendingTimestamps(_.timestamp * 1000L)


        //2.过滤出pv行为
        val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")

        //3.转换数据格式（uv,用户id）
        //第一个给uv，是因为keyBy后分到一个分组里，调用分组的聚合算子
        //因为后面要用用户id去重，所以不能给1，要给用户id
        val uvDS: DataStream[(String, Long)] = filterDS.map(data => ("uv", data.userId))

        //4.根据uv进行分组
        val uvKS: KeyedStream[(String, Long), String] = uvDS.keyBy(_._1)
        //5.自定义
        //process，使用Set对userId去重，Set的长度就是uv数
        uvKS
                .timeWindow(Time.hours(1)) //当数据量特别大的时候 所有数据会进入一个窗口 会造成问题
                .process(new MyProcessWindowFunction())
                .print("uv")

        env.execute()

    }

    class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), Long, String, TimeWindow] {
        private var uvSet = mutable.Set[Long]()

        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[Long]): Unit = {
            for (element <- elements) {
                uvSet.add(element._2)
            }
            out.collect(uvSet.size)
            uvSet.clear()   //记得清空
        }
    }


    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}
