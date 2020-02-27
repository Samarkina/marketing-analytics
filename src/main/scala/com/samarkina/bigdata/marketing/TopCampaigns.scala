package com.samarkina.bigdata.marketing

import com.samarkina.bigdata.{MobileAppClick, Purchase}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

// Task 2.1
object TopCampaigns {
  def averagePlainSQL(spark: SparkSession, purchaseDataset: Dataset[Purchase], mobileAppClickDataset: Dataset[MobileAppClick]) = {
    purchaseDataset.createOrReplaceTempView("Purchases")
    mobileAppClickDataset.createOrReplaceTempView("Clicks")

    spark.sql(
      """
        |SELECT c.channelId, AVG(p.billingCost) AS cost
        |FROM Purchases AS p
        |JOIN Clicks AS c
        |ON p.purchaseId = c.purchaseId
        |WHERE p.purchaseId IS NOT NULL
        |AND p.isConfirmed = "TRUE"
        |GROUP BY c.channelId
        |ORDER BY Cost DESC
        |LIMIT 10
        |""".stripMargin).show()
  }

  def averageDataFrame(spark: SparkSession, purchaseDataset: Dataset[Purchase], mobileAppClickDataset: Dataset[MobileAppClick]) = {
    import spark.implicits._
    val avgTable = purchaseDataset.as("Purchases").join(
      mobileAppClickDataset.as("Clicks"),
      col("Purchases.purchaseId") === col("Clicks.purchaseId"),
      "left"
    )
      .filter("Purchases.purchaseId IS NOT NULL")
      .filter(("""Purchases.isConfirmed = "TRUE" """ ))
      .orderBy($"billingCost".desc)
      .groupBy("Clicks.channelId")
      .agg(
        avg($"Purchases.billingCost").as("cost")
      )
        .select(
          "Clicks.channelId",
          "cost"
        )
    avgTable
      .show(100)

  }
}
