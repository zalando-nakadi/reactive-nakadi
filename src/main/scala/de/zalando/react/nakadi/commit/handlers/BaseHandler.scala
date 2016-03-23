package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.NakadiMessages.Topic
import de.zalando.react.nakadi.commit.OffsetTracking

import scala.concurrent.Future

trait BaseHandler {

  def commitSync(groupId: String, topic: Topic, offsets: Seq[OffsetTracking]): Future[Unit]

  def readCommit(groupId: String, topic: Topic, partitionId: String): Future[Option[OffsetTracking]]

}
