package com.samarkina.bigdata

import java.sql.Timestamp

case class MobileAppClick(
  userId: String,
  eventId: String,
  eventTime: Timestamp,
  eventType: String,
  attributes: Option[Map[String, String]]
)