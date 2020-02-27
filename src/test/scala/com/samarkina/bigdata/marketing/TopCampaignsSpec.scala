package com.samarkina.bigdata.marketing

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TopCampaignsSpec extends AnyWordSpec with Matchers with SparkContextSetup {
  val pathClicks = "src/test/resources/mobile-app-clickstream_sample-mobile-app-clickstream_sample.csv"
  val pathPurchase = "src/test/resources/purchases_sample-purchases_sample.csv"

  "Task 2.1. Compare two DataSets" in withSparkContext { spark =>
      val (purchaseDataset, mobileAppClickDataset) = ReadFiles.reading(spark, pathClicks, pathPurchase)
      val first = TopCampaigns.averagePlainSQL(spark, purchaseDataset, mobileAppClickDataset)
      val second = TopCampaigns.averageDataFrame(spark, purchaseDataset, mobileAppClickDataset)
      first.collect() should contain theSameElementsAs (second.collect())
    }

}
