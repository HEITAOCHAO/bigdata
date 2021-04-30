package com.heitaoc.bigdata.core.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: 郭超
 * @DateTime: 2021/4/29 11:20
 */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    //建立spark连接
    var conf = new SparkConf().setMaster("local").setAppName("wordCount")
    var sc = new SparkContext(conf)

    //1.读取文件一行一行读取
    val value: RDD[String] = sc.textFile("data\\*")

    //2.将一行拆分为一个一个的单词
    val words: RDD[String] = value.flatMap(_.split(" "))

    //3.把 单词 转换成 （hello,1）
    val wordToOne: RDD[(String, Int)] = words.map(word => {
      (word, 1)
    })

    //4.单词分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(word => word._1)

    //5.对分组后的数据进行转换 ，结构的转换一般都要使用.map (聚合)
    val wordCount: RDD[(String, Int)] = groupRDD.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (word, t1._2 + t2._2)
          }
        )
      }
    }
    //6.打印结果 将转换结果采集到控制台
    val tuples: Array[(String, Int)] = wordCount.collect()
    tuples.foreach(println)

    //关闭连接
    sc.stop()

  }
}
