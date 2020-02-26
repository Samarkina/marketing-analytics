package com.samarkina.bigdata

import java.sql.Timestamp

case class TargetSchema (
  purchase: Purchase,
  // a session starts with app_open event and finishes with app_close
  sessionId: String,
  campaignId: String, // derived from app_open#attributes#campaign_id
  channelIid: String // derived from app_open#attributes#channel_id
)