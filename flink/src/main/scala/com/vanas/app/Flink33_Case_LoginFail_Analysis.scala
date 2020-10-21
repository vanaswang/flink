package com.vanas.app

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示。这是电商网站、也是几乎所有网站风控的基本一环。
 *
 *  恶意登陆监控
 *
 *  存在问题：
 *   1. 数据乱序
 *   2. 只保存了上一次的失败：如果需求连续失败5次、10次呢
 *   3. 连续怎么保证？ => 不能过滤
 *
 * @author Vanas
 * @create 2020-08-13 10:47 上午 
 */
object Flink33_Case_LoginFail_Analysis {
    def main(args: Array[String]): Unit = {
        // 流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 1. 读取数据，并封装成样例类
        val logDS= env.readTextFile("input/LoginLog.csv")

        // 2. 转换成样例类
        val loginDS: DataStream[LoginEvent] = logDS
                .map(
                    data => {
                        val datas: Array[String] = data.split(",")
                        LoginEvent(
                            datas(0).toLong,
                            datas(1),
                            datas(2),
                            datas(3).toLong
                        )
                    }
                )
                .assignTimestampsAndWatermarks(
                    new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
                        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
                    }
                )
        //3.过滤出失败的记录
        val filterDS = loginDS.filter(_.eventType == "fail")
        //4.按照用户ID分组
        val longinKS: KeyedStream[LoginEvent, Long] = filterDS.keyBy(_.userId)

        //5.对用户的恶意登入行为进行处理
        longinKS.process(
            new KeyedProcessFunction[Long, LoginEvent, String] {
                //保存上一次失败的记录
                private var lastLoginEvent: ValueState[LoginEvent] = _

                override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, String]#Context, collector: Collector[String]): Unit = {
                    val lastFailEvent: LoginEvent = lastLoginEvent.value()
                    if (lastFailEvent == null) {
                        //为null，表示当前数据是第一条失败的数据，保存到状态
                        lastLoginEvent.update(value)
                    } else {
                        //不为null，表示上一条失败数据已存在，判断时间是否超过2s
                        if (value.eventTime - lastFailEvent.eventTime <= 2) {

                            collector.collect("用户" + value.userId + "在2秒内连续2次登入失败，可能为恶意登入")
                        }
                        lastLoginEvent.clear()
                    }
                }
            }
        )
                .print("login error")
        env.execute()

    }

    /**
     * 登陆日志样例类
     *
     * @param userId    用户ID
     * @param ip        用户的IP
     * @param eventType 事件类型：成功、失败
     * @param eventTime 事件时间
     */
    case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

}
