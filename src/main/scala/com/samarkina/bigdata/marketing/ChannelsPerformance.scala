package com.samarkina.bigdata.marketing

import com.samarkina.bigdata.{MobileAppClick, Purchase}
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

// Task 2.2
object ChannelsPerformance {

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
        |""".stripMargin).show()
  }

  def highestAmountDataFrame(spark: SparkSession, purchaseDataset: Dataset[Purchase], mobileAppClickDataset: Dataset[MobileAppClick]) = {
    import spark.implicits._
    val innerTable = mobileAppClickDataset.as("Clicks")
      .select("channelId", "sessionId")
      .distinct()

    val highestAmountTable = innerTable
      .select("channelId")
      .groupBy("channelId")
      .count()

    highestAmountTable.show()

  }

}
