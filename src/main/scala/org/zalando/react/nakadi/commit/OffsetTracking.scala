package org.zalando.react.nakadi.commit

import org.joda.time.DateTime

case class OffsetTracking(
  partitionId: String,
  checkpointId: String,
  leaseHolder: String,
  leaseTimestamp: DateTime,
  leaseCounter: Option[Long] = None,
  leaseId: Option[String] = None
)
