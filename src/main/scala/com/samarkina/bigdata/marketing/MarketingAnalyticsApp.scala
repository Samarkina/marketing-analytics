package com.samarkina.bigdata.marketing
import com.samarkina.bigdata.{MobileAppClick, Purchase}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
object MarketingAnalyticsApp {
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
  def getTargetTable(purchaseDataset: Dataset[Purchase], mobileAppClickDataset: Dataset[MobileAppClick]) = {
    purchaseDataset.as("pr").join(
      mobileAppClickDataset.as("mac"),
      col("pr.purchaseId") === col("mac.purchaseId"),
      "left"
    )
      .filter("pr.purchaseId IS NOT NULL")
      .select(
        col("pr.purchaseId"),
        col("purchaseTime"),
        col("billingCost"),
        col("isConfirmed"),
        col("sessionId"),
        col("campaignId"),
        col("channelId")
      ).show(100)
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
      .csv("src/main/resources/mobile-app-clickstream_sample-mobile-app-clickstream_sample.csv")

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
      .csv("src/main/resources/purchases_sample-purchases_sample.csv")
      .withColumn("billingCost", 'billingCost.cast(DoubleType))
      .as[Purchase]

    // Task 1.1
    getTargetTable(purchaseDataset, mobileAppClickDataset)

    // Task 2.1
    TopCampaigns.averagePlainSQL(spark, purchaseDataset, mobileAppClickDataset)
    TopCampaigns.averageDataFrame(spark, purchaseDataset, mobileAppClickDataset)
//
//    // Task 2.2
    ChannelsPerformance.highestAmountPlainSQL(spark, purchaseDataset, mobileAppClickDataset)
    ChannelsPerformance.highestAmountDataFrame(spark, purchaseDataset, mobileAppClickDataset)


  }
}