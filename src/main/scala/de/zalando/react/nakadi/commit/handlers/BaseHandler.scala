package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.commit.OffsetTracking

import scala.concurrent.Future

trait BaseHandler {

  def commitSync(groupId: String, topic: String, offsets: Seq[OffsetTracking]): Future[Unit]

  def readCommit(groupId: String, topic: String, partitionId: String): Future[Option[OffsetTracking]]

}
