package de.zalando.react.nakadi.client.models


case class Cursor(
  partition: String,
  offset: String
)

case class EventStreamBatch(
  cursor: Cursor,
  events: Option[Seq[Event]] = None
)
