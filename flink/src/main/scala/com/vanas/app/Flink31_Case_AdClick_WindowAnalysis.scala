package com.vanas.app

import com.vanas.common.SimpleAggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author Vanas
 * @create 2020-08-12 7:38 下午 
 */
object Flink31_Case_AdClick_WindowAnalysis {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        // 1. 读取数据，并封装成样例类
        val logDS: DataStream[String] = env.readTextFile("input/AdClickLog.csv")

        // 2. 转换成样例类
        val adClickDS: DataStream[AdClickLog] = logDS
                .map(
                    data => {
                        val datas: Array[String] = data.split(",")
                        AdClickLog(
                            datas(0).toLong,
                            datas(1).toLong,
                            datas(2),
                            datas(3),
                            datas(4).toLong
                        )
                    }
                )
                .assignAscendingTimestamps(_.timestamp * 1000L)
        //3.根据省份、广告进行分组
        val proAdKS: KeyedStream[AdClickLog, (Long, Long)] = adClickDS.keyBy(
            adClick => (adClick.userId, adClick.adId)
        )

        //4.增加时间窗口，每5秒统计最近10分钟的数据
        proAdKS
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(
                    new SimpleAggregateFunction[AdClickLog],
                    new ProcessWindowFunction[Long, ProAd, (Long, Long), TimeWindow] {
                        override def process(key: (Long, Long), context: Context, elements: Iterable[Long], out: Collector[ProAd]): Unit = {
                            out.collect(ProAd(key._1, key._2, elements.iterator.next(), context.window.getEnd))
                        }
                    }
                )
                .keyBy(_.windowEnd)
                .process(
                    new KeyedProcessFunction[Long, ProAd, String] {
                        override def processElement(i: ProAd, context: KeyedProcessFunction[Long, ProAd, String]#Context, collector: Collector[String]): Unit = {
                            collector.collect(
                                """
                                  |${value.userId}-${value.adId}点击了${value.totalCount}次
                                  |""".stripMargin)
                        }
                    }
                ).print("ad")
        env.execute()

    }

    case class ProAd(userId: Long, adId: Long, totalCount: Long, windowEnd: Long)

    /**
     * 广告点击样例类
     *
     * @param userId    用户ID
     * @param adId      广告ID
     * @param province  省份
     * @param city      城市
     * @param timestamp 时间戳
     */
    case class AdClickLog(
                                 userId: Long,
                                 adId: Long,
                                 province: String,
                                 city: String,
                                 timestamp: Long)

}
