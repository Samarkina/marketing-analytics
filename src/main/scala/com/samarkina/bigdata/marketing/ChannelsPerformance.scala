package com.samarkina.bigdata.marketing

import com.samarkina.bigdata.{MobileAppClick, Purchase}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Task 2.2
  * Finds the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)
  *  with the App in each campaign.
  */
object ChannelsPerformance {

  /**
    * Creates a DataFrame which channelId has the most sessionId counters using Plain SQL.
    *
    * @param spark SparkSession
    * @param purchaseDataset contains Purchases
    * @param mobileAppClickDataset contains MobileAppClicks
    * @return sql.DataFrame with channelId and count of the channelId
    */
  def highestAmountPlainSQL(spark: SparkSession, purchaseDataset: Dataset[Purchase], mobileAppClickDataset: Dataset[MobileAppClick]) = {
    purchaseDataset.createOrReplaceTempView("Purchases")
    mobileAppClickDataset.createOrReplaceTempView("Clicks")

    spark.sql(
      """
        |SELECT channelId, COUNT(channelId) AS count
        |FROM
        |(
        |   SELECT DISTINCT c.channelId, c.sessionId
        |   FROM Clicks AS c
        |)
        |GROUP BY channelId
        |""".stripMargin)
  }

  /**
    * Creates a DataFrame whose channelId has the most sessionId counters using Datasets
    *
    * @param purchaseDataset contains Purchases
    * @param mobileAppClickDataset contains MobileAppClicks
    * @return sql.DataFrame with channelId and count of the channelId
    */
  def highestAmountDataFrame(purchaseDataset: Dataset[Purchase], mobileAppClickDataset: Dataset[MobileAppClick]) = {
    val innerTable = mobileAppClickDataset.as("Clicks")
      .select("channelId", "sessionId")
      .distinct()

    val highestAmountTable = innerTable
      .select("channelId")
      .groupBy("channelId")
      .count()

    highestAmountTable

  }

}
