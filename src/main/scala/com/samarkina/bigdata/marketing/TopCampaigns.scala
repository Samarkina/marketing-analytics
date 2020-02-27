package com.samarkina.bigdata.marketing

import org.apache.spark.sql.{DataFrame, SparkSession}

// Task 2.1
object TopCampaigns {
  def averagePlainSQL(spark: SparkSession, purchaseDf: DataFrame, mobileAppClick2: DataFrame) = {
    purchaseDf.createOrReplaceTempView("Purchases")
    mobileAppClick2.createOrReplaceTempView("Clicks")

    spark.sql(
      """
        |SELECT c.channel_id AS Name, AVG(p.billingCost) AS Cost
        |FROM Purchases AS p
        |JOIN Clicks AS c
        |ON p.purchaseId = c.purchase_id
        |WHERE p.purchaseId IS NOT NULL
        |AND p.isConfirmed = "TRUE"
        |GROUP BY c.channel_id
        |ORDER BY Cost DESC
        |LIMIT 10
        |""".stripMargin).show()
  }
}
