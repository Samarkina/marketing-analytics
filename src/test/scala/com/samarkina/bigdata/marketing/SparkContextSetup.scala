package com.samarkina.bigdata.marketing

import org.apache.spark.sql.SparkSession

trait SparkContextSetup {
  def withSparkContext(testMethod: (SparkSession) => Any) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config("spark.master", "local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      testMethod(spark)
    }
    finally spark.stop()
  }
}