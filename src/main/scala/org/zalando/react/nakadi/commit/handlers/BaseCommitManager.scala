package org.zalando.react.nakadi.commit.handlers

import org.zalando.react.nakadi.commit.OffsetTracking

import scala.concurrent.Future

trait BaseCommitManager {

  def put(groupId: String, eventType: String, offset: OffsetTracking): Future[OffsetTracking]

  def get(groupId: String, eventType: String, partitionId: String): Future[Option[OffsetTracking]]
}
