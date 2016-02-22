package de.zalando.react.nakadi.client.models


case class Cursor(
  partition: String,
  offset: String
)

case class EventStreamBatch(
  cursor: Option[Cursor] = None,
  events: Option[Seq[String]] = None
)
