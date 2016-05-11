package org.zalando.react.nakadi

import org.joda.time.DateTime


package object commit {

  case class OffsetTracking(
    partitionId: String,
    checkpointId: String,
    leaseHolder: String,
    leaseTimestamp: DateTime,
    leaseCounter: Option[Long] = None,
    leaseId: Option[String] = None
  )
}