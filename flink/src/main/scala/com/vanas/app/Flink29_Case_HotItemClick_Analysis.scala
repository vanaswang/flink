package com.vanas.app

import java.sql.Timestamp
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 每隔5分钟输出最近一小时内点击量最多的前N个商品
 * @author Vanas
 * @create 2020-08-12 4:17 下午 
 */
object Flink29_Case_HotItemClick_Analysis {
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
                //2.指定抽取的事件时间
                .assignAscendingTimestamps(_.timestamp * 1000L)


        //3.过滤出点击行为
        val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")

        //4.按照商品ID进行分组
        val userBehaviorKS: KeyedStream[UserBehavior, Long] = filterDS.keyBy(_.itemId)

        //5.增加窗口范围
        val userBehaviorWS: WindowedStream[UserBehavior, Long, TimeWindow] = userBehaviorKS.timeWindow(Time.hours(1), Time.minutes(5))


        //6.按窗口聚合，并打上窗口时间信息，用于后续实现：按照窗口分组的效果
        //怎么求和？
        //怎么排序？
        //userBehaviorWS.sum()
        //userBehaviorWS.reduce()
        //userBehaviorWS.process()
        //aggregate 传2个参数：
        //第一个参数：是一个AggregateFunction，实现预聚合操作
        //第二个参数：ProcessWindowFunction，全窗口函数，可以拿到窗口信息
        //第一个预聚合的结果，会传递给全窗口函数
        //预聚合之后，数据量变得非常小，比如说（100x，xx） 有100万条数据，聚合之后变成 （1001，100），（1002，200）汇总后的数据，数据量很小，等于商品种类的数量

        val hotItemClickDS: DataStream[HotItemClick] = userBehaviorWS.aggregate(new MyAggregateFunction(), new MyProcessWindowFunction)

        //7.按照窗口时间进行分组：因为topN的范围是每个窗口（1小时）
        val hotItemClickKS: KeyedStream[HotItemClick, Long] = hotItemClickDS.keyBy(_.windowEnd)

        //8.在分组内（同一个时间窗口的数据），进行排序，取topN
        //因为数据都是预聚合过的，所以数据量非常小 可以直接sort不用担心oom
        hotItemClickKS.process(new MyKeyedProcessFunction()).print("top3")


        env.execute()
    }

    class MyKeyedProcessFunction extends KeyedProcessFunction[Long, HotItemClick, String] {
        //我们要进行排序，就要把所有（同一窗口）数据先存起来
        //因为是多条数据，那么就要用List
        private var dataList: ListState[HotItemClick] = _
        //为了不重复注册定时器，给一个状态来保存定时器的时间，用于判断是否已经注册过定时器
        private var triggerTimer: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
            dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotItemClick]("dataList", classOf[HotItemClick]))
            triggerTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTime", classOf[Long]))

        }

        /**
         * 来一条数据执行一次处理
         *
         * @param value
         * @param context
         * @param collector
         */
        override def processElement(value: HotItemClick, context: KeyedProcessFunction[Long, HotItemClick, String]#Context, collector: Collector[String]): Unit = {
            //来一条存一条
            dataList.add(value)
            //什么时候才是所有数据都来齐了？=>模拟窗口出发（窗口结束时间，进行计算操作）=>注册一个窗口结束时间的定时器
            if (triggerTimer.value() == 0) {
                context.timerService().registerProcessingTimeTimer(value.windowEnd)
                triggerTimer.update(value.windowEnd)
            }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotItemClick, String]#OnTimerContext, out: Collector[String]): Unit = {
            //1.取出状态中保存的数据
            val datas: util.Iterator[HotItemClick] = dataList.get().iterator()

            //2.取出的状态传递给scala中的集合，为了可以调用sort方法
            val listBuffer = new ListBuffer[HotItemClick]
            while (datas.hasNext) {
                val data: HotItemClick = datas.next()
                listBuffer += data
            }
            //3.状态使用完，先清空，先把内存释放出来
            dataList.clear()
            triggerTimer.clear()

            //4.直接对集合中的数据进行排序（降序）、取前N个
            val top3: ListBuffer[HotItemClick] = listBuffer.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

            //5.将数据发送
            out.collect(
                s"""
                   |窗口时间：${new Timestamp(timestamp)}
                   |-------------------------------------
                   |${top3.mkString("\n")}
                   |-------------------------------------
                   |""".stripMargin
            )
        }
    }

    /**
     * 自定义预聚合函数
     * 他的输出会传递给ProcessWindowFunction
     */
    class MyAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: UserBehavior, acc: Long): Long = acc + 1L

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
    }

    /**
     * 自定义 全窗口函数
     * 目的：给聚合后的数据加上窗口时间的信息
     * IN：输入类型，就是预聚合函数的输出类型
     */
    class MyProcessWindowFunction extends ProcessWindowFunction[Long, HotItemClick, Long, TimeWindow] {
        override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[HotItemClick]): Unit = {
            out.collect(HotItemClick(key, elements.iterator.next(), context.window.getEnd))
        }
    }


    case class HotItemClick(itemId: Long, clickCount: Long, windowEnd: Long)

    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}
