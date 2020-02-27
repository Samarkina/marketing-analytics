package com.samarkina.bigdata.marketing
import com.samarkina.bigdata.{MobileAppClick, Purchase, TargetSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Main class of the MarketingAnalyticsApp
  */
object MarketingAnalyticsApp {

  /**
    * Task 1.1
    * Returns purchases attribution projection based on incoming datasets.
    *
    * @param spark SparkSession
    * @param purchaseDataset contains Purchases
    * @param mobileAppClickDataset contains MobileAppClicks
    * @return Target Dataset for Task 1.1
    */
  def getTargetTable(spark: SparkSession, purchaseDataset: Dataset[Purchase], mobileAppClickDataset: Dataset[MobileAppClick]) = {
    import spark.implicits._
    purchaseDataset.as("p").join(
      mobileAppClickDataset.as("c"),
      col("p.purchaseId") === col("c.purchaseId"),
      "left"
    )
      .filter("p.purchaseId IS NOT NULL")
      .select(
        col("p.purchaseId") as "purchaseId",
        col("purchaseTime"),
        col("billingCost"),
        col("isConfirmed"),
        col("sessionId"),
        col("campaignId"),
        col("channelId")
      ).as[TargetSchema]
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config("spark.master", "local[2]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val pathClicks = "src/main/resources/mobile-app-clickstream_sample-mobile-app-clickstream_sample.csv"
    val pathPurchase = "src/main/resources/purchases_sample-purchases_sample.csv"

    val (purchaseDataset, mobileAppClickDataset) = ReadFiles.readDatasets(spark, pathClicks, pathPurchase)

    // Task 1.1
    val targetDataset = getTargetTable(spark, purchaseDataset, mobileAppClickDataset)

    targetDataset.printSchema()
    targetDataset.show()

    // Task 2.1
    TopCampaigns.averagePlainSQL(spark, purchaseDataset, mobileAppClickDataset).show()
    TopCampaigns.averageDataFrame(spark, purchaseDataset, mobileAppClickDataset).show()

   // Task 2.2
    ChannelsPerformance.highestAmountPlainSQL(spark, purchaseDataset, mobileAppClickDataset).show()
    ChannelsPerformance.highestAmountDataFrame(purchaseDataset, mobileAppClickDataset).show()


    spark.stop()
  }
}