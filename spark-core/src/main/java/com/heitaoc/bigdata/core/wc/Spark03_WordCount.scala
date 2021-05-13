package com.heitaoc.bigdata.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 郭超
 * @DateTime: 2021/4/29 11:20
 */
object Spark03_WordCount {
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

    //4.spark提供了更多功能，可以将分组和聚合使用一个方法实现
    //相同的reduceByKey:相同key的数据，可以对value进行reduce聚合
    //wordToOne.reduceByKey((x,y)=>{x+y})   匿名函数自检原则
    //wordToOne.reduceByKey((x,y)=>x+y)    匿名函数自检原则
    val wordCount = wordToOne.reduceByKey(_+_)



    //5.打印结果 将转换结果采集到控制台
    val tuples: Array[(String, Int)] = wordCount.collect()
    tuples.foreach(println)

    //关闭连接
    sc.stop()

  }
}
