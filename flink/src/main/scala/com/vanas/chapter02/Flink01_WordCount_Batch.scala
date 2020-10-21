package com.vanas.chapter02

import org.apache.flink.api.scala._

/**
 * @author Vanas
 * @create 2020-08-04 9:28 上午
 */
object Flink01_WordCount_Batch {
    def main(args: Array[String]): Unit = {


        //1.创建执行环境
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //2。读取数据
        val lineDS: DataSet[String] = env.readTextFile("/Users/vanas/Desktop/vanas/flink0213/flink/src/main/resources/word.txt")

        //3.数据的逻辑处理
        //3.1 对读取的数据进行扁平化处理
        val wordDS: DataSet[String] = lineDS.flatMap(_.split(" "))
        //转换（word，1）
        val word2OneDS: DataSet[(String, Int)] = wordDS.map((_, 1))
        //按照word2OneDs分组
        // java.lang.UnsupportedOperationException: Aggregate does not support grouping with KeySelector functions, yet.
        val groupDS: GroupedDataSet[(String, Int)] = word2OneDS.groupBy(0)
        //3.4按照结果进行求和
        val sumDS: AggregateDataSet[(String, Int)] = groupDS.sum(1)
        //4.执行
        sumDS.print()

    }

}
