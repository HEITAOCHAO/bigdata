package com.heitaoc.bigdata.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @Author: 郭超
 * @DateTime: 2021/4/29 9:57
 */
object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    //建立spark连接
    var conf = new SparkConf().setMaster("local").setAppName("wordCount")
    var sc = new SparkContext(conf)

    //1.读取文件一行一行读取
    val value: RDD[String] = sc.textFile("data\\*")

    //2.将一行拆分为一个一个的单词
    val words: RDD[String] = value.flatMap(_.split(" "))

    //3.单词分组
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //4.对分组后的数据进行转换 ，结构的转换一般都要使用.map
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    //5.打印结果 将转换结果采集到控制台
    val tuples: Array[(String, Int)] = wordToCount.collect()
    tuples.foreach(println)


    //关闭连接
    sc.stop()

  }

}
