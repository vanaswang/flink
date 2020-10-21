package com.vanas.app

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author Vanas
 * @create 2020-08-13 1:44 下午 
 */
object Flink34_Case_OrderTx_TimeOutAnalysis {
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


        //2.按订单ID分组
        val orderKS: KeyedStream[OrderEvent, Long] = orderDS.keyBy(_.orderId)
        //3.进行超时分析
        val processDS = orderKS.process(
            new KeyedProcessFunction[Long, OrderEvent, String] {
                private var payData: ValueState[OrderEvent] = _
                private var createData: ValueState[OrderEvent] = _
                private var alarmTimer: ValueState[Long] = _
                val outputTag = new OutputTag[String]("timeout")


                override def open(parameters: Configuration): Unit = {
                    payData = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payData", classOf[OrderEvent]))
                    createData = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("createData", classOf[OrderEvent]))
                    alarmTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("alarmTimer", classOf[Long]))


                }

                override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]): Unit = {
                    // 每条数据来的时候，就应该加上一个定时器
                    if (alarmTimer.value() == 0) {
                        // 最好是使用系统时间，进行注册
                        //            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000)
                        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000)
                        alarmTimer.update(ctx.timerService().currentProcessingTime() + 15 * 60 * 1000)
                    } else {
                        // 如果定时器已经注册过了：代表同一个订单的create、pay数据来齐了=> 删除定时器、清空状态
                        //            ctx.timerService().deleteProcessingTimeTimer(alarmTimer.value())
                        ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
                        alarmTimer.clear()
                    }

                    if (value.eventType == "create") {
                        //1 来的数据是create
                        val pay: OrderEvent = payData.value()
                        if (pay == null) {
                            // 1.1 pay还没来过，临时保存create状态
                            createData.update(value)
                        } else {
                            // 1.2 pay已经来了,判断时间是否超时
                            if (pay.eventTime - value.eventTime > 15 * 60) {
                                // 1.2.1 虽然支付了，但是时间上是超时的
                                ctx.output(outputTag, "订单" + value.orderId + "支付成功，但是时间超时")
                            } else {
                                // 1.2.2 正常支付
                                out.collect("订单" + value.orderId + "支付成功")
                                payData.clear()
                            }
                        }

                    } else {
                        //2 来的数据是pay,判断一下create来没来
                        val create: OrderEvent = createData.value()
                        if (create == null) {
                            // 2.1 create还没来:临时保存pay的状态
                            payData.update(value)
                        } else {
                            // 2.2 create已经来了:判断是否超时
                            if (value.eventTime - create.eventTime > 15 * 60) {
                                // 2.2.1 超时了：放到测输出流
                                ctx.output(outputTag, "订单" + create.orderId + "支付成功，但是时间超时")
                            } else {
                                // 2.2.2 没超时，正常支付了
                                out.collect("订单" + value.orderId + "支付成功")
                                createData.clear()
                            }
                        }
                    }
                }

                // 定时器触发：代表，另一条数据超过时间了，还没来 => 数据丢失 or 下单不支付
                override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
                    if (payData.value() != null) {
                        // 1.如果pay有状态，说明之前pay来过，那么就是create超时没来
                        ctx.output(outputTag, "订单" + payData.value().orderId + "有支付数据，但下单数据丢失，数据异常！")
                    }
                    if (createData.value() != null) {
                        // 2. 如果create有状态，说明之前create来过，那么就是pay超时了还没来：有可能是本身就不支付，有可能是数据丢失
                        ctx.output(outputTag, "订单" + createData.value().orderId + "支付超时")
                    }

                    payData.clear()
                    createData.clear()
                    alarmTimer.clear()
                }
            }
        )
        processDS.print("normal")
        val outputTag = new OutputTag[String]("timeout")
        processDS.getSideOutput(outputTag).print("timeout")


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
