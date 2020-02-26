package com.samarkina.bigdata

import java.sql.Timestamp

case class Purchase(
 purchaseId: String,
 purchaseTime: Timestamp,
 billingCost: Double,
 isConfirmed: Boolean
)