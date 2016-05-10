package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.commit.OffsetTracking

import scala.concurrent.Future

trait BaseLeaseManager {

  def put(groupId: String, eventType: String, offset: OffsetTracking): Future[OffsetTracking]

  def get(groupId: String, eventType: String, partitionId: String): Future[Option[OffsetTracking]]
}
