package org.zalando.react.nakadi.properties


case class ProducerProperties(
  serverProperties: ServerProperties,
  tokenProvider: Option[() => String],
  eventType: String
)
