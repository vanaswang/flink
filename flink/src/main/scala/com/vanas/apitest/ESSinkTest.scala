package com.vanas.apitest

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * @author Vanas
 * @create 2020-08-07 4:42 下午 
 */
object ESSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/sensor.txt")
        val dataStream = inputStream.map(data => {
            val arr: Array[String] = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        //定义httphosts
        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("hadoop102", 9200))

        //自定义写入es的RSSinkFunction
        val myESSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
            override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                //包装一个Map作为data source
                val dataSource = new util.HashMap[String, String]()
                dataSource.put("id", t.id)
                dataSource.put("temperature", t.temperature.toString)
                dataSource.put("ts", t.timestramp.toString)
                //窗前index request,用于发送http请求
                val indexRequest = Requests.indexRequest()
                        .index("sensor")
                        .`type`("readingdata")
                        .source(dataSource)
                //用indexer发送请求
                requestIndexer.add(indexRequest)
            }
        }

        dataStream.addSink(
            new ElasticsearchSink.Builder[SensorReading](httpHosts, myESSinkFunc).build())


        env.execute("es sink test")

    }

}
