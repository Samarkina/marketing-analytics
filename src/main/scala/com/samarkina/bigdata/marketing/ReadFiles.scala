package com.samarkina.bigdata.marketing

import com.samarkina.bigdata.{MobileAppClick, Purchase}
//import com.samarkina.bigdata.marketing.MarketingAnalyticsApp.{campaignChannelSetter, sessionIdSetter}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object ReadFiles {
  def campaignChannelSetter = {
    var currentId = ""
    (campaignChannelId: String) => {
      if (campaignChannelId != null)
        currentId = campaignChannelId
      currentId
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

  def reading(spark: SparkSession, pathClicks: String, pathPurchase: String): (Dataset[Purchase], Dataset[MobileAppClick]) = {

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
      .csv(pathPurchase)
      .withColumn("billingCost", 'billingCost.cast(DoubleType))
      .as[Purchase]

    (purchaseDataset, mobileAppClickDataset)
  }

}
