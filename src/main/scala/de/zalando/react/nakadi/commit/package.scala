package de.zalando.react.nakadi

import org.joda.time.DateTime


package object commit {
  type Offset = Long
  type Offsets = Map[TopicPartition, Offset]

  case class OffsetTrackingWrites(
    partitionId: String,
    checkpointId: String,
    leaseHolder: String,
    leaseCounter: Long,
    leaseTimestamp: DateTime,
    leaseId: Option[String] = None
  )

  case class OffsetTrackingReads(
    partitionId: String,
    checkpointId: String,
    leaseHolder: String,
    leaseTimestamp: DateTime,
    leaseId: Option[String] = None
  )
}