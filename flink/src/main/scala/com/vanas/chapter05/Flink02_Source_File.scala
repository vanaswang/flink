package com.vanas.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @author Vanas
 * @create 2020-08-05 10:28 上午 
 */
object Flink02_Source_File {
    def main(args: Array[String]): Unit = {

        //val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //流式处理环境
//        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        //从文件中读取数据
        val sourceDS = env.readTextFile("/")
        sourceDS.print()
        env.execute()

    }

}
