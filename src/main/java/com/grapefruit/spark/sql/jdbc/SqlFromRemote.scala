/*
 * Copyright @2022 Grapefruit. All rights reserved.
 */

package com.grapefruit.spark.sql.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取mysql数据
 *
 * @Author ZhangZhihuang
 * @Date 2022/7/23 12:51
 * @Version 1.0
 */
object SqlFromRemote {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    runJdbcDatasetExample(spark)
  }

  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/grapefruit?useUnicode=true&characterEncoding=UTF-8&useSSL=false")
      .option("dbtable", "t_markdown")
      .option("user", "root")
      .option("password", "123456")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load()

    // create table
    jdbcDF.createOrReplaceTempView("t_markdown")

    // read data
    jdbcDF.sqlContext.sql("select * from t_markdown where title = '端组件库大合集' or title = 'log4j漏洞重现'").show()
  }
}
