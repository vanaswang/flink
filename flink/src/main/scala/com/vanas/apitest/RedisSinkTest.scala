package com.vanas.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author Vanas
 * @create 2020-08-07 3:51 下午 
 */
object RedisSinkTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream: DataStream[String] = env.readTextFile("/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt")
        val dataStream: DataStream[SensorReading] = inputStream.map(data => {
            val arr: Array[String] = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })


        //定义一个FlinkJedisConfigBase
        val conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build()


        dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))
        env.execute("redis sink")

    }

}

//定义一个RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading] {
    //定义保存数据写入redis的命令，HSET 表名 key value
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")

    //将温度值指定为value
    override def getKeyFromData(data: SensorReading): String = data.id

    //将id指定为key
    override def getValueFromData(data: SensorReading): String = data.temperature.toString

}