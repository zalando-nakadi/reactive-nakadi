package org.zalando.react.nakadi.client.models

import org.joda.time.DateTime


case class EventMetadata(
  eid: String,
  event_type: Option[String] = None,
  occurred_at: DateTime,
  received_at: Option[DateTime] = None,
  parent_eids: Option[Seq[String]] = None,
  flow_id: Option[String] = None
)

sealed trait Event {
  def metadata: EventMetadata
}

case class BusinessEvent(
  metadata: EventMetadata,
  payload: EventPayload
) extends Event

case class DataChangeEvent(
  data_type: String,
  data_op: DataOpEnum.DataOp,
  data: EventPayload, // Raw payload for event
  metadata: EventMetadata
) extends Event

case class Cursor(
  partition: String,
  offset: String
)

case class EventStreamBatch(
  cursor: Cursor,
  events: Option[Seq[Event]] = None
)

case class EventTypeStatistics(
  expectedWriteRate: Option[Long] = None,
  messageSize: Option[Long] = None,
  readParallelism: Option[Long] = None,
  writeParallelism: Option[Long] = None
)

case class EventType(
  name: String,
  statistics: Option[EventTypeStatistics] = None,
  partitionKeyFields: Seq[String],
  dataKeyFields: Option[Seq[String]] = None,
  owningApplication: String,
  validationStrategies: Option[Seq[String]] = None,
  partitionResolutionStrategy: Option[play.api.libs.json.JsValue] = None,
  schema: Option[play.api.libs.json.JsValue] = None,
  category: EventTypeCategoryEnum.Category,
  enrichmentStrategies: Seq[String]
)
