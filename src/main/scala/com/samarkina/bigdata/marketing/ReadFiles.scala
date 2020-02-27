package com.samarkina.bigdata.marketing

import com.samarkina.bigdata.{MobileAppClick, Purchase}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Contains utility functions for reading datasets.
  */
object ReadFiles {

  /**
    * Sets appropriate channelId and campaignId values where column is null in current App Session.
    */
  def campaignChannelSetter = {
    var currentId = ""
    (campaignChannelId: String) => {
      if (campaignChannelId != null)
        currentId = campaignChannelId
      currentId
    }
  }

  /**
    * Fills sessionId column based on usedId and current sessions count.
    * Example of generated sessionId: u1_s1.
    */
  def sessionIdSetter = {
    var inc = 0
    (eventType: String, userId: String) => {
      if (eventType == "app_open")
        inc += 1
      userId + "_s" + inc
    }
  }

  /**
    * Reads files and creates datasets based on their contents.
    *
    * @param spark SparkSession
    * @param pathClicks path to Mobile App Clickstream file
    * @param pathPurchase path to Purchases file
    * @return Two datasets with Purchase and MobileAppClick types
    */
  def readDatasets(spark: SparkSession, pathClicks: String, pathPurchase: String): (Dataset[Purchase], Dataset[MobileAppClick]) = {

    import spark.implicits._

    val schemaAppOpen = StructType(Array(
      StructField("campaign_id", StringType, true),
      StructField("channel_id", StringType, true)
    ))

    val schemaPurchase = StructType(Array(
      StructField("purchase_id", StringType, true)
    ))

    val sessionIdSet = spark.udf.register("sessionIdSetter", sessionIdSetter).asNondeterministic()
    val channelSetter = spark.udf.register("channelSetter", campaignChannelSetter).asNondeterministic()
    val campaignSetter = spark.udf.register("campaignSetter", campaignChannelSetter).asNondeterministic()

    val mobileAppClickDataset = spark.read
      .option("escape", "\"")
      .option("header", "true")
      .csv(pathClicks)

      .select(col("userId"), col("eventId"), col("eventTime"), col("eventType"),
        from_json(col("attributes"), schemaAppOpen).as("jsonData"),
        from_json(col("attributes"), schemaPurchase).as("jsonData2"))
      .select("userId", "eventId", "eventTime", "eventType", "jsonData.*", "jsonData2.*")

      .withColumn("sessionId", sessionIdSet(col("eventType"), col("userId")))
      .withColumn("channelId", channelSetter(col("channel_id")))
      .withColumn("campaignId", campaignSetter(col("campaign_id")))

      .withColumnRenamed("purchase_id", "purchaseId")
      .as[MobileAppClick]

    val purchaseDataset = spark.read
      .option("escape", "\"")
      .option("header", "true")
      .schema(Encoders.product[Purchase].schema)
      .csv(pathPurchase)
      .as[Purchase]

    (purchaseDataset, mobileAppClickDataset)
  }

}
