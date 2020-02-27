package com.samarkina.bigdata.marketing

import com.samarkina.bigdata.Purchase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object MarketingAnalyticsApp {

  def campaignChannelSetter = {
    var channel_id_global = ""
    (campaign_channel_id: String) => {
      if (campaign_channel_id != null)
        channel_id_global = campaign_channel_id
      channel_id_global
    }
  }

  def sessionIdSetter = {
    var inc = 0
    (eventType: String, userId: String) => {
      if (eventType == "app_open")
        inc += 1
      userId + "_s" + inc
    }
  }

  def getTargetTable(purchaseDf: DataFrame, mobileAppClick2: DataFrame) = {
    val target = purchaseDf.join(
      mobileAppClick2,
      col("purchaseId") === col("purchase_id"),
      "left"
    )
      .filter("purchaseId IS NOT NULL")

    target.select(
      col("purchaseId"),
      col("purchaseTime"),
      col("billingCost"),
      col("isConfirmed"),
      col("sessionId"),
      col("campaign_id"),
      col("channel_id")
    ) .show(100)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
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

    val sessionIdSet = spark.udf.register("sessionIdSetter", sessionIdSetter).asNondeterministic()

    val mobileAppClick = dfFromCSVJSON.withColumn("sessionId", sessionIdSet(col("eventType"), col("userId")))

    val channelSetter = spark.udf.register("channelSetter", campaignChannelSetter).asNondeterministic()

    val campaignSetter = spark.udf.register("campaignSetter", campaignChannelSetter).asNondeterministic()

    val mobileAppClick2 = mobileAppClick
      .withColumn("channel_id", channelSetter(col("channel_id")))
      .withColumn("campaign_id", campaignSetter(col("campaign_id")))

    val purchaseDf = spark.read
      .option("escape", "\"")
      .option("header", "true")
      .csv("src/main/resources/purchases_sample-purchases_sample.csv")
      .withColumn("billingCost", 'billingCost.cast(DoubleType))


    // Task 1.1
    getTargetTable(purchaseDf, mobileAppClick2)

    // Task 2.1
    TopCampaigns.averagePlainSQL(spark, purchaseDf, mobileAppClick2)







  }
}