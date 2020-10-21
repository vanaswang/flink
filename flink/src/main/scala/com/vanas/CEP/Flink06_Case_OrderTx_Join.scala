package com.vanas.CEP

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author Vanas
 * @create 2020-08-13 3:32 下午 
 */
object Flink06_Case_OrderTx_Join {
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

        val orderKS: KeyedStream[OrderEvent, String] = orderDS.keyBy(_.txId)


        val txDS: DataStream[TxEvent] = env.readTextFile("input/ReceiptLog.csv")
                .map(
                    line => {
                        val datas: Array[String] = line.split(",")
                        TxEvent(datas(0), datas(1), datas(2).toLong)
                    }
                )
                .assignAscendingTimestamps(_.eventTime * 1000L)

        val txKS: KeyedStream[TxEvent, String] = txDS.keyBy(_.txId)




        // 2.Join两条按照交易码分组后的流
        orderKS
                .intervalJoin(txKS)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(
                    new ProcessJoinFunction[OrderEvent, TxEvent, (OrderEvent, TxEvent)] {
                        override def processElement(left: OrderEvent, right: TxEvent, ctx: ProcessJoinFunction[OrderEvent, TxEvent, (OrderEvent, TxEvent)]#Context, out: Collector[(OrderEvent, TxEvent)]): Unit = {
                            if (left.txId == right.txId) {
                                out.collect((left, right))
                            }
                        }
                    }
                )
                .print("interval join")

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

    /**
     * 交易系统数据样例类
     *
     * @param txId       交易码
     * @param payChannel 支付渠道：微信、支付宝
     * @param eventTime  事件事件
     */
    case class TxEvent(txId: String, payChannel: String, eventTime: Long)


}
