package com.samarkina.bigdata.marketing

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.apache.spark.sql.functions._

class ReadFilesSpec extends AnyWordSpec with Matchers with SparkContextSetup {
  val pathClicks = "src/test/resources/mobile-app-clickstream_sample-mobile-app-clickstream_sample.csv"
  val pathPurchase = "src/test/resources/purchases_sample-purchases_sample.csv"


  "ReadFiles. CampaignChannelSetter" in withSparkContext { spark =>
    val (purchaseDataset, mobileAppClickDataset) = ReadFiles.readDatasets(spark, pathClicks, pathPurchase)

    import spark.implicits._

    mobileAppClickDataset.filter($"channelId" === null).collect() shouldBe empty
  }

  "ReadFiles. sessionIdSetter" in withSparkContext { spark =>
    val (purchaseDataset, mobileAppClickDataset) = ReadFiles.readDatasets(spark, pathClicks, pathPurchase)

    import spark.implicits._

    val countAppCloseTable = mobileAppClickDataset
      .filter($"eventType" === "app_close")
      .count()

    val maxSession = mobileAppClickDataset
    .agg(
      max($"sessionId").as("sessionId")
    )

    val maxSessionNumber:Int = maxSession
      .withColumn("sessionId2", regexp_extract($"sessionId", "(u\\d{1,})_s(\\d{1,})" , 2))
      .first()
      .getAs[String]("sessionId2")
      .toInt

    assert(countAppCloseTable == maxSessionNumber)

  }
}