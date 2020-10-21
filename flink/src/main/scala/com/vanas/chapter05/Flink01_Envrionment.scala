package com.vanas.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @author Vanas
 * @create 2020-08-05 10:28 上午 
 */
object Flink01_Envrionment {
    def main(args: Array[String]): Unit = {

        //val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //流式处理环境
//        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        //从集合中读取数据
        val sourceDS: DataStream[Int] = env.fromCollection(
            List(1, 2, 3, 4, 5,6,7,8,9,10,2,1,2)
        )
        sourceDS.print()
        env.execute()

    }

}
