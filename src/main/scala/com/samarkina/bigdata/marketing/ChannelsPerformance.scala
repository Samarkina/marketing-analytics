package com.samarkina.bigdata.marketing

import org.apache.spark.sql.{DataFrame, SparkSession}

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
}
