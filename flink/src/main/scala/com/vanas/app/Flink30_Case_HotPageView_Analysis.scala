package com.vanas.app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.vanas.common.SimpleAggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
 * @author Vanas
 * @create 2020-08-12 6:53 下午 
 */
object Flink30_Case_HotPageView_Analysis {
    def main(args: Array[String]): Unit = {

        // 流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        // 1. 读取数据，并封装成样例类
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val apacheLogDS: DataStream[ApacheLog] = env
                .readTextFile("input/apache.log")
                .map(
                    line => {
                        val datas: Array[String] = line.split(" ")
                        ApacheLog(
                            datas(0),
                            datas(1),
                            sdf.parse(datas(3)).getTime,
                            datas(5),
                            datas(6)
                        )
                    }
                )

        // 2. 指定抽取的事件时间 (时间乱序大概1分钟左右)
        val timeDS: DataStream[ApacheLog] = apacheLogDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
                override def extractTimestamp(element: ApacheLog): Long = element.eventTime
            }
        )
        //3.按照url进行分组
        val apacheLogKS: KeyedStream[ApacheLog, String] = timeDS.keyBy(_.url)
        //4.增加时间窗口
        val apacheLogWS: WindowedStream[ApacheLog, String, TimeWindow] = apacheLogKS.timeWindow(Time.minutes(10), Time.seconds(5))
        //5.按照窗口进行预聚合，打上窗口结束时间的标记
        val aggDS: DataStream[HotPageview] = apacheLogWS.aggregate(
            new SimpleAggregateFunction[ApacheLog],
            new ProcessWindowFunction[Long, HotPageview, String, TimeWindow] {
                override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[HotPageview]): Unit = {
                    out.collect(HotPageview(key, elements.iterator.next(), context.window.getEnd))
                }
            }
        )
        //6.按照标记的窗口结束时间进行分组
        aggDS.keyBy(_.windowEnd)
                .process(
                    new KeyedProcessFunction[Long, HotPageview, String] {

                        private var dataList: ListState[HotPageview] = _
                        private var triggerTimer: ValueState[Long] = _


                        override def open(parameters: Configuration): Unit = {
                            dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotPageview]("dataList", classOf[HotPageview]))
                            triggerTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTimer", classOf[Long]))
                        }

                        override def processElement(value: HotPageview, context: KeyedProcessFunction[Long, HotPageview, String]#Context, collector: Collector[String]): Unit = {
                            //来一条存一条
                            dataList.add(value)
                            //注册一个定时器
                            if (triggerTimer.value() == 0) {
                                context.timerService().registerEventTimeTimer(value.windowEnd)
                                triggerTimer.update(value.windowEnd)
                            }
                        }

                        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotPageview, String]#OnTimerContext, out: Collector[String]): Unit = {
                            val datas: util.Iterator[HotPageview] = dataList.get().iterator()
                            val listBuffer = new ListBuffer[HotPageview]
                            while (datas.hasNext) {
                                listBuffer.append(datas.next())
                            }
                            dataList.clear()
                            triggerTimer.clear()

                            val top3: ListBuffer[HotPageview] = listBuffer.sortWith(_.clickCount > _.clickCount).take(3)
                            out.collect(
                                s"""
                                   |窗口结束时间：${new Timestamp(timestamp)}
                                   |----------------------------------------
                                   |${top3.mkString("\n")}
                                   |---------------------------------------
                                   |""".stripMargin
                            )
                        }
                    }
                )
                .print("top3 page view")
        env.execute()
    }


    case class HotPageview(url: String, clickCount: Long, windowEnd: Long)

    case class ApacheLog(
                                ip: String,
                                userId: String,
                                eventTime: Long,
                                method: String,
                                url: String)

}
