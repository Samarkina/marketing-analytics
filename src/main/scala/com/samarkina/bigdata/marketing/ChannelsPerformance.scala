package com.samarkina.bigdata.marketing

import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

// Task 2.2
object ChannelsPerformance {

  def highestAmountPlainSQL(spark: SparkSession, purchaseDf: DataFrame, mobileAppClick2: DataFrame) = {
    purchaseDf.createOrReplaceTempView("Purchases")
    mobileAppClick2.createOrReplaceTempView("Clicks")

    spark.sql(
      """
        |SELECT channel_id, COUNT(channel_id)
        |FROM
        |(
        |   SELECT DISTINCT c.channel_id, c.sessionId
        |   FROM Clicks AS c
        |)
        |GROUP BY channel_id
        |""".stripMargin).show()
  }

  def highestAmountDataFrame(spark: SparkSession, purchaseDf: DataFrame, mobileAppClick2: DataFrame) = {
    val purchaseDfasPurchases = purchaseDf.as("Purchases")
    val mobileAppClickasClicks = mobileAppClick2.as("Clicks")

    import spark.implicits._
    val innerTable = mobileAppClickasClicks
      .select("channel_id", "sessionId")
      .distinct()

    val highestAmountTable = innerTable
      .select("channel_id")
      .groupBy("channel_id")
      .count()

    highestAmountTable.show()

  }

}
