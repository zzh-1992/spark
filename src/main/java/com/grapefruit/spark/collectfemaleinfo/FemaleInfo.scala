package com.grapefruit.spark.collectfemaleinfo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 华为云MRS教程
 * https://support.huaweicloud.com/devg-mrs/mrs_06_0176.html
 *
 * @Author ZhangZhihuang
 * @Date 2022/7/10 12:51
 * @Version 1.0
 */
object CollectFemaleInfo {
  //表结构，后面用来将文本数据映射为df
  case class FemaleInfo(name: String, gender: String, stayTime: Int)

  def main(args: Array[String]): Unit = {
    //配置Spark应用名称
    val sparkConf = new SparkConf().setMaster("local").setAppName("FemaleInfo")
    val sc = new SparkContext(sparkConf)

    val sqlContext = SparkSession.builder.config(sparkConf).getOrCreate().sqlContext

    import sqlContext.implicits._
    //通过隐式转换，将RDD转换成DataFrame，然后注册表
    sc.textFile("logs").map(_.split(","))
      .map(p => FemaleInfo(p(0), p(1), p(2).trim.toInt))
      .toDF.createOrReplaceTempView("FemaleInfoTable")

    //通过sql语句筛选女性上网时间数据, 对相同名字行进行聚合
    val femaleTimeInfo = sqlContext.sql("select name,sum(stayTime) as stayTime from FemaleInfoTable" +
      " where gender = 'female' group by name")
    //筛选出时间大于两个小时的女性网民信息，并输出
    val c = femaleTimeInfo.filter("stayTime >= 120").collect()
    c.foreach(println)
    sc.stop()
  }
}
