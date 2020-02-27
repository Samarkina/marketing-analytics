package com.samarkina.bigdata

import java.sql.Timestamp

/**
  * Purchases attribution projection schema.
  *
  * @param purchaseId (example: p1)
  * @param purchaseTime (example: 2019-01-01 0:01:05)
  * @param billingCost (example: 100.5)
  * @param isConfirmed (example: TRUE)
  * @param sessionId (example: u1_s1) a session starts with app_open event and finishes with app_close
  * @param campaignId (example: cmp1) derived from app_open#attributes#campaign_id
  * @param channelId (example: Google Ads) derived from app_open#attributes#channel_id
  */
case class TargetSchema (
  purchaseId: String,
  purchaseTime: Timestamp,
  billingCost: Double,
  isConfirmed: Boolean,
  sessionId: String,
  campaignId: String,
  channelId: String
)