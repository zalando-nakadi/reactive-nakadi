package org.zalando.react.nakadi.commit

import java.time.OffsetDateTime

case class OffsetTracking(
  partitionId: String,
  checkpointId: String,
  leaseHolder: String,
  leaseTimestamp: OffsetDateTime,
  leaseCounter: Option[Long] = None,
  leaseId: Option[String] = None
)
