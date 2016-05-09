package de.zalando.react.nakadi.properties


case class ProducerProperties(
  serverProperties: ServerProperties,
  tokenProvider: Option[() => String],
  topic: String
)
