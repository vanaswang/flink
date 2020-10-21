package com.vanas.chapter02

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
/**
 * @author Vanas
 * @create 2020-08-04 10:06 上午
 */
object Flink03_WordCount_UnBoundedStream {
    def main(args: Array[String]): Unit = {
        //1.创建执行环境

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(8) //设置并行度
        //从外部命令中提取参数，作为socket主机名和端口号
        val paramTool =ParameterTool.fromArgs(args)
        val host: String = paramTool.get("host")
        val port: Int = paramTool.getInt("port")


        //2.读取数据：socket、kafka
        val inputDataStream: DataStream[String] = env.socketTextStream("host", port)

        //3.数据逻辑处理
        //        val wordDS: DataStream[String] = lineDS.flatMap(_.split(" "))
        //        val word2OneDS: DataStream[(String, Int)] = wordDS.map((_, 1))
        //        val wordKS: KeyedStream[(String, Int), String] = word2OneDS.keyBy(_._1)
        //        val sumDS: DataStream[(String, Int)] = wordKS.sum(1)
        val resultDataStream: DataStream[(String, Int)] = inputDataStream
                .flatMap(_.split(" ")).slotSharingGroup("2")
                .filter(_.nonEmpty).disableChaining()
                .map((_, 1)).setMaxParallelism(3).startNewChain()
                .keyBy(0)
                .sum(1).setBufferTimeout(2)


        resultDataStream.print().setParallelism(1)
        //4.启动执行环境
        env.execute()


    }

}
