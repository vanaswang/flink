package com.vanas.CEP

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Vanas
 * @create 2020-08-13 2:38 下午 
 */
object Flink04_Case_LoginFail_CEP {
    def main(args: Array[String]): Unit = {
        // 流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 1. 读取数据，并封装成样例类
        val logDS: DataStream[String] = env.readTextFile("input/LoginLog.csv")

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
        // 3. 按照用户ID进行分组
        val loginKS: KeyedStream[LoginEvent, Long] = loginDS.keyBy(_.userId)
        //4使用CEP来统计恶意登陆事件
        //4.1定义规则
        val pattern: Pattern[LoginEvent, LoginEvent] = Pattern
                .begin[LoginEvent]("start").where(_.eventTime == "fail")
                // .next("next").where(_.eventType == "fail")
                .times(2, 3)
                .within(Time.seconds(3))

        //4.2 应用规则
        val loginPS: PatternStream[LoginEvent] = CEP.pattern(loginKS, pattern)
        //4.3获取匹配上的规则数据
//        loginPS.select(
//            data => {
//                data.toString()
//            }
//        )
//                .print("login fail detect")

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
