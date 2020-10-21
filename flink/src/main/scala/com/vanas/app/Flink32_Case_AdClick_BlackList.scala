package com.vanas.app

import com.vanas.common.SimpleAggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author Vanas
 * @create 2020-08-12 8:35 下午 
 */
object Flink32_Case_AdClick_BlackList {
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

        // 3. 根据用户、广告进行分组
        val proAdKS: KeyedStream[AdClickLog, (Long, Long)] = adClickDS.keyBy(
            adClick => (adClick.userId, adClick.adId)
        )

        // 4. 应该对恶意刷单行为进行监测过滤
        val blackFilterDS: DataStream[AdClickLog] = proAdKS.process(
            new KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
                private var dailyClickCount: ValueState[Long] = _
                private var clearTimer: ValueState[Long] = _
                // 告警标记：用来是表示是否已经发送过告警信息
                private var alarmFlag: ValueState[Boolean] = _

                override def open(parameters: Configuration): Unit = {
                    dailyClickCount = getRuntimeContext.getState(new ValueStateDescriptor[Long]("dailyClickCount", classOf[Long]))
                    clearTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("clearTimer", classOf[Long]))
                    alarmFlag = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("alarmFlag", classOf[Boolean]))
                }

                override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
                    // 来一条数据,计数+1
                    val lastClickCount: Long = dailyClickCount.value()

                    // 当第一条数据来的时候，计算隔天0点的时间戳,
                    // 并且要按照处理时间来注册定时器 => 因为不管有没有数据来，到了0点，就要触发清空操作
                    if (lastClickCount == 0) {
                        // 在第二天0点的时候，对状态清零 => 定时器
                        // 怎么知道什么时候是第二天0点？ => 取整的思想 =>  现在的时间戳 / 一天的时间戳
                        val currentDay: Long = ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24)
                        // 获取明天的天数
                        val nextDay: Long = currentDay + 1
                        // 将明天0：00时间转成时间戳
                        val clearTime: Long = nextDay * (1000 * 60 * 60 * 24)

                        if (clearTimer.value() == 0) {
                            ctx.timerService().registerProcessingTimeTimer(clearTime)
                            clearTimer.update(clearTime)
                        }
                    }


                    if (lastClickCount >= 100) {
                        if (!alarmFlag.value()) {
                            val outputTag = new OutputTag[String]("blackList")
                            ctx.output(outputTag, "用户" + value.userId + "恶意点击" + value.adId + "广告,达到本日阈值100")
                            alarmFlag.update(true)
                        }
                        return
                    } else {
                        dailyClickCount.update(lastClickCount + 1L)
                        out.collect(value)
                    }
                }

                override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
                    dailyClickCount.clear()
                    alarmFlag.clear()
                    clearTimer.clear()
                }
            }
        )

        // 输出侧输出流中的告警信息
        val outputTag = new OutputTag[String]("blackList")
        blackFilterDS.getSideOutput(outputTag).print("blackList")

        // 5、对黑名单过滤完的数据，进行正常的处理,需要重新进行分组
        blackFilterDS
                .keyBy(data => (data.userId, data.adId))
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(
                    new SimpleAggregateFunction[AdClickLog],
                    new ProcessWindowFunction[Long, UserAd, (Long, Long), TimeWindow] {
                        override def process(key: (Long, Long), context: Context, elements: Iterable[Long], out: Collector[UserAd]): Unit = {
                            out.collect(UserAd(key._1, key._2, elements.iterator.next(), context.window.getEnd))
                        }
                    }
                )
                .keyBy(_.windowEnd)
                .process(
                    new KeyedProcessFunction[Long, UserAd, String] {
                        override def processElement(value: UserAd, ctx: KeyedProcessFunction[Long, UserAd, String]#Context, out: Collector[String]): Unit = {
                            out.collect(value.userId + "-" + value.adId + "点击了" + value.totalCount + "次")
                        }
                    }
                )
                .print("user-ad click count")

        env.execute()

    }

    case class UserAd(userId: Long, adId: Long, totalCount: Long, windowEnd: Long)


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
