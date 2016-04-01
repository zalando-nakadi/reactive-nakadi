package de.zalando.react.nakadi.client.models

import org.joda.time.DateTime


case class MetaData(
  eid: String,
  event_type: Option[String] = None,
  occurred_at: DateTime,
  received_at: Option[DateTime] = None,
  parent_eids: Option[Seq[String]] = None,
  flow_id: Option[String] = None
)

case class Event(
  data_type: String,
  data_op: DataOpEnum.DataOp,
  data: EventPayload, // Raw payload for event
  metadata: MetaData
)

case class Cursor(
  partition: String,
  offset: String
)

case class EventStreamBatch(
  cursor: Cursor,
  events: Option[Seq[Event]] = None
)
