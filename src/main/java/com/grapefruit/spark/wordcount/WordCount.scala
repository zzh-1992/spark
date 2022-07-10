package com.grapefruit.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取远端服务器hdfs上文件完成wordcount
 * 华为云教程:https://education.huaweicloud.com/courses/course-v1:HuaweiX+CBUCNXE191+Self-paced/courseware/dc012e1b702a48b3b90f577c086190a2/c756921f99f44577aa519f3cab938ec3/
 * 尚硅谷教程:https://www.bilibili.com/video/BV11A411L7CK?p=7&vd_source=7081221b8ebd4b672a4f92a4f661fc85
 * @Author ZhangZhihuang
 * @Date 2022/7/10 12:51
 * @Version 1.0
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    // 创建SparkContext对象，设置应用名称WordCount
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // 从hdfs加载文本文件，得到RDD数据集
    val textFile = sc.textFile("hdfs://node01:8020/text.txt")

    // 调用RDD的transformation进行计算
    // 将文本文件按照逗号分割，
    // 然后每个单词计数设置为1
    // 最后按照相同的key计数求和
    // 这一步会分发到各个Executor上执行
    val count = textFile
      // 以逗号分割
      .flatMap(line => line.split(","))
      // 分组
      .map(word => (word, 1))
      // 规约
      .reduceByKey(_ + _)

    // 保存结果(调用action操作，保存结果这一行才触发真正的任务执行)
    count.saveAsTextFile("hdfs://node01:8020/spark")

    // close
    sc.stop();
  }
}
