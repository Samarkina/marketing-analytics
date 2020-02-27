package com.samarkina.bigdata

import java.sql.Timestamp

/**
  * Represents user click.
  *
  * @param userId (example: u1)
  * @param eventId (example: u1_e1)
  * @param eventTime (example: 2019-01-01 0:00:00)
  * @param eventType (example: app_open)
  * @param channelId (example: Google Ads) derived from app_open#attributes#channel_id
  * @param campaignId (example: cmp1) derived from app_open#attributes#campaign_id
  * @param sessionId (example: u1_s1) a session starts with app_open event and finishes with app_close
  */
case class MobileAppClick(
  userId: String,
  eventId: String,
  eventTime: Timestamp,
  eventType: String,
  channelId: String,
  campaignId: String,
  sessionId: String
)