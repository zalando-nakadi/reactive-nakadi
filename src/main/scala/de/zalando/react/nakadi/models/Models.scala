package de.zalando.react.nakadi.models


case class PartitionResolutionStrategy(
  name: String,
  doc: Option[String] = None
)

case class EventEnrichmentStrategy(
  name: String,
  doc: Option[String] = None
)

case class DataChangeEventQualifier(
  dataType: String,
  dataOp: DataChangeEventQualifierDataOpEnum.DataOp
)

case class Problem(
  instance: Option[String] = None,
  status: Int,
  detail: Option[String] = None,
  title: String,
  `type`: String
)

case class EventValidationStrategy(
  name: String,
  doc: Option[String] = None
)

case class Partition(
  partition: String,
  oldestAvailableOffset: String,
  newestAvailableOffset: String
)

case class EventTypeSchema(
  `type`: String,
  schema: String
)

case class Cursor(
  partition: String,
  offset: String
)

case class EventMetadataMetadata(
  eid: Option[String] = None,
  eventType: Option[String] = None,
  occurredAt: Option[org.joda.time.DateTime] = None,
  rootEid: Option[java.util.UUID] = None,
  receivedAt: Option[org.joda.time.DateTime] = None,
  flowId: Option[String] = None,
  parentEid: Option[java.util.UUID] = None
)

case class EventType(
  name: String,
  dataKeyFields: Option[Seq[String]] = None,
  owningApplication: Option[String] = None,
  effectiveSchema: Option[String] = None,
  orderingKeyField: Option[String] = None,
  validationStrategies: Option[Seq[EventValidationStrategy]] = None,
  partitionResolutionStrategy: Option[PartitionResolutionStrategy] = None,
  schema: Option[EventTypeSchema] = None,
  category: EventTypeCategoryEnum.Category,
  enrichmentStrategies: Option[Seq[EventEnrichmentStrategy]] = None
)

// FIXME - events should be unmarshalled to `Event` class
case class EventStreamBatch(
  cursor: Option[Cursor] = None,
  events: Seq[String] = Nil
)

case class EventMetadata(
  metadata: EventMetadataMetadata
)


