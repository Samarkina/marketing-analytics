package com.samarkina.bigdata.marketing
import com.samarkina.bigdata.{MobileAppClick, Purchase}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
object MarketingAnalyticsApp {

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

    val pathClicks = "src/main/resources/mobile-app-clickstream_sample-mobile-app-clickstream_sample.csv"
    val pathPurchase = "src/main/resources/purchases_sample-purchases_sample.csv"

    val (purchaseDataset, mobileAppClickDataset) = ReadFiles.reading(spark, pathClicks, pathPurchase)

    // Task 1.1
    getTargetTable(purchaseDataset, mobileAppClickDataset)

    // Task 2.1
    TopCampaigns.averagePlainSQL(spark, purchaseDataset, mobileAppClickDataset)
    TopCampaigns.averageDataFrame(spark, purchaseDataset, mobileAppClickDataset)

   // Task 2.2
    ChannelsPerformance.highestAmountPlainSQL(spark, purchaseDataset, mobileAppClickDataset)
    ChannelsPerformance.highestAmountDataFrame(spark, purchaseDataset, mobileAppClickDataset)


  }
}