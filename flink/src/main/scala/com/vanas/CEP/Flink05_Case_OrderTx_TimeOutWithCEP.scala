package com.vanas.CEP

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Vanas
 * @create 2020-08-13 3:00 下午 
 */
object Flink05_Case_OrderTx_TimeOutWithCEP {
    def main(args: Array[String]): Unit = {
        // 流处理执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 1. 读取数据，并封装成样例类
        val orderDS: DataStream[OrderEvent] = env.readTextFile("input/OrderLog.csv")
                .map(
                    line => {
                        val datas: Array[String] = line.split(",")
                        OrderEvent(
                            datas(0).toLong,
                            datas(1),
                            datas(2),
                            datas(3).toLong
                        )
                    }
                )
                .assignAscendingTimestamps(_.eventTime * 1000L)

        // 2. 按订单ID分组
        val orderKS: KeyedStream[OrderEvent, Long] = orderDS.keyBy(_.orderId)
        // 3. 使用CEP进行超时分析
        // 3.1 定义规则
        // CEP没法处理数据异常的情况
        val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
                .begin[OrderEvent]("create").where(_.eventType == "create")
                .followedBy("pay").where(_.eventType == "pay")
                .within(Time.minutes(15))
        // 3.2 应用规则
        val orderPS: PatternStream[OrderEvent] = CEP.pattern(orderKS, pattern)

        // 3.3 取出数据
        // select 可以传3个参数
        //  第一个，侧输出流的标签，用来存储匹配超时的数据
        //  第二个，超时数据的处理函数，用来定义怎么处理超时的数据，之后放入侧输出流；这就说明了它保存了超时的数据
        //  第三个，匹配上规则的数据的处理函数
        val outputTag = new OutputTag[String]("timeout")
        val resultDS: DataStream[String] = orderPS.select(outputTag)(
            (timeoutDatas, ts) => {
                timeoutDatas.toString()
            }
        )(
            data => {
                data.toString()
            }
        )

        resultDS.getSideOutput(outputTag).print("timeout")


        env.execute()
    }


    /**
     * 订单系统数据样例类
     *
     * @param orderId   订单ID
     * @param eventType 事件类型：创建、支付
     * @param txId      交易码
     * @param eventTime 事件时间
     */
    case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)


}
