package de.zalando.react.nakadi


case class ProducerProperties(
  serverProperties: ServerProperties,
  tokenProvider: Option[() => String],
  topic: String
)