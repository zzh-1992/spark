package com.grapefruit.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取本地文件完成wordcount
 *
 * @Author ZhangZhihuang
 * @Date 2022/7/10 12:51
 * @Version 1.0
 */
object WordCount02 {

  def main(args: Array[String]): Unit = {

    // conncet with spark
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // read file(get line data)
    val lines: RDD[String] = sc.textFile("datas")

    // split the line data with ","
    val words = lines.flatMap(_.split(","))

    // group by the word
    val wordGroup = words.groupBy(word => word)

    // map the group work:List => word:int
    val wordCount = wordGroup.map {
      case (word, list) =>
        (word, list.size)
    }

    // collect data
    val array = wordCount.collect()

    // print every element
    array.foreach(println)

    // close source
    sc.stop();
  }
}
