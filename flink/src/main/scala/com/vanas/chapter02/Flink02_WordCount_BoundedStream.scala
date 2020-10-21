package com.vanas.chapter02

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
/**
 * @author Vanas
 * @create 2020-08-04 9:28 上午
 */
object Flink02_WordCount_BoundedStream {
    def main(args: Array[String]): Unit = {

        //1.创建执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //2。读取数据
        val lineDS: DataStream[String] = env.readTextFile("/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/word.txt")

        //3.数据的逻辑处理
        //3.1 对读取的数据进行扁平化处理
        val wordDS: DataStream[String] = lineDS.flatMap(_.split(" "))
        //转换（word，1）
        val word2OneDS: DataStream[(String, Int)] = wordDS.map((_, 1))
        //按照word进行分组
        val keyDS: KeyedStream[(String, Int), Tuple] = word2OneDS.keyBy(0)
        //3.4按照w
        val sumDS: DataStream[(String, Int)] = keyDS.sum(1)
        //4.执行
        sumDS.print()


        //5.启动执行环境
        env.execute()

    }

}
