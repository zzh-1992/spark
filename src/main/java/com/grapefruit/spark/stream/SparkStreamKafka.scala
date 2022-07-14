/*
 * Copyright @2022 Grapefruit. All rights reserved.
 */

package com.grapefruit.spark.stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * 从kafka读取数据(消息批处理)
 * 华为云教程-Spark Streaming Write To Print代码样例 :https://support.huaweicloud.com/devg-mrs/mrs_06_0180.html
 *
 * @Author ZhangZhihuang
 * @Date 2022/7/13 22:50
 * @Version 1.0
 */
object SparkStreamKafka {
  def main(args: Array[String]): Unit = {
    // 参数解析:
    // <topics>为Kafka中订阅的主题，多以逗号分隔。
    // <brokers>为获取元数据的kafka地址。
    //val Array(batchTime, windowTime, topics, brokers) = args

    // <batchTime>为Streaming分批的处理间隔。
    val batchTime: String = "1"

    // <windowTime>为统计数据的时间跨度,时间单位都是秒。
    val windowTime: String = "1"

    val batchDuration: Duration = Seconds(batchTime.toInt)
    val windowDuration: Duration = Seconds(windowTime.toInt)

    // 建立Streaming启动环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkStreamKafka")

    sparkConf.setAppName("DataSightStreamingExample")

    val ssc: StreamingContext = new StreamingContext(sparkConf, batchDuration)

    // 设置Streaming的CheckPoint目录，由于窗口概念存在，该参数必须设置
    ssc.checkpoint("checkpoint")

    // 组装Kafka的主题列表
    val topicsSet: Set[String] = "grapefruit".split(",").toSet

    // 通过brokers和topics直接创建kafka stream
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "group.id" -> "tweet-consumer"
    )

    // 1.接收Kafka中数据，生成相应DStream
    val lines: DStream[String] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    ).map((_: ConsumerRecord[String, String]).value())

    // 2.获取每一个行的字段属性
    val records: DStream[(String, String, Int)] = lines.map(getRecord)

    // 3.筛选女性网民上网时间数据信息
    val femaleRecords: DStream[(String, Int)] = records.filter((_: (String, String, Int))._2 == "female")
      .map((x: (String, String, Int)) => (x._1, x._3))

    // 4.汇总在一个时间窗口内每个女性上网时间
    val aggregateRecords: DStream[(String, Int)] = femaleRecords
      .reduceByKeyAndWindow((_: Int) + (_: Int), (_: Int) - (_: Int), windowDuration)

    // 5.筛选连续上网时间超过阈值的用户，并获取结果
    aggregateRecords.filter((_: (String, Int))._2 > 0.9 * windowTime.toInt).print()

    // 6.Streaming系统启动
    ssc.start()
    ssc.awaitTermination()
  }

  // 获取字段函数
  def getRecord(line: String): (String, String, Int) = {
    val elems: Array[String] = line.split(",")
    val name: String = elems(0)
    val sexy: String = elems(1)
    val time: Int = elems(2).toInt
    (name, sexy, time)
  }
}
