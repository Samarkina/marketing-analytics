package com.samarkina.bigdata

import java.sql.Timestamp

/**
  * Contains purchase data.
  *
  * @param purchaseId (example: p1)
  * @param purchaseTime (example: 2019-01-01 0:01:05)
  * @param billingCost (example: 100.5)
  * @param isConfirmed (example: TRUE)
  */
case class Purchase(
 purchaseId: String,
 purchaseTime: Timestamp,
 billingCost: Double,
 isConfirmed: Boolean
)