package com.vanas.common

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * @author Vanas
 * @create 2020-08-12 7:03 下午 
 */
class SimpleAggregateFunction[T] extends AggregateFunction[T, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: T, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b

}
