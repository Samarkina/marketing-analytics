package com.samarkina.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object MarketingAnalyticsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val dfFromCSV = spark.read
      .option("escape", "\"")
      .option("header", "true")
      .csv("src/main/resources/mobile-app-clickstream_sample-mobile-app-clickstream_sample.csv")


    val schemaAppOpen = StructType(Array(
      StructField("campaign_id",StringType,true),
      StructField("channel_id",StringType,true)
    ))

    val schemaPurchase = StructType(Array(
      StructField("purchase_id",StringType,true)
    ))


    val dfFromCSVJSON =  dfFromCSV.select(col("userId"), col("eventId"), col("eventTime"), col("eventType"),
      from_json(col("attributes"),schemaAppOpen).as("jsonData"),
      from_json(col("attributes"),schemaPurchase).as("jsonData2")
    )
      .select("userId","eventId", "eventTime", "eventType", "jsonData.*", "jsonData2.*")
    dfFromCSVJSON.printSchema()
    dfFromCSVJSON.show(false)

  }
}