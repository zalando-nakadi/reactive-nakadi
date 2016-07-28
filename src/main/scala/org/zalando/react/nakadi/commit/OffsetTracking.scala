package org.zalando.react.nakadi.commit

import java.time.ZonedDateTime

case class OffsetTracking(
  partitionId: String,
  checkpointId: String,
  leaseHolder: String,
  leaseTimestamp: ZonedDateTime,
  leaseCounter: Option[Long] = None,
  leaseId: Option[String] = None
)
