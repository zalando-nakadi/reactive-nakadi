package de.zalando.react.nakadi

import com.typesafe.config.Config
import de.zalando.react.nakadi.utils.IdGenerator
import de.zalando.react.nakadi.commit.{OffsetMap, OffsetTracking}
import de.zalando.react.nakadi.commit.handlers.{BaseHandler => BaseCommitHandler}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


trait LeaseManager {

  def leaseId: String

  def leaseHolder: String

  def counter: mutable.Map[String, Long]

  def isLeaseAvailable(groupId: String, topic: String, partitionId: String)(implicit executionContext: ExecutionContext): Future[Boolean]

  def commit(groupId: String, topic: String, offsetMap: OffsetMap)(implicit executionContext: ExecutionContext): Future[Unit]

}

class LeaseManagerImpl(override val leaseHolder: String,
                       commitHandler: BaseCommitHandler,
                       staleLeaseDelta: FiniteDuration,
                       idGenerator: IdGenerator) extends LeaseManager {

  def newLeaseTimeout: DateTime = new DateTime(DateTimeZone.UTC).plusSeconds(staleLeaseDelta.length.toInt)

  // Key / value for partition id and lease counter
  override val counter: mutable.Map[String, Long] = mutable.Map.empty
  override val leaseId: String = idGenerator.generate

  private def execCommit(groupId: String,
                         topic: String,
                         offsetTracking: Seq[OffsetTracking])
                        (implicit executionContext: ExecutionContext): Future[Unit] = {

    commitHandler.put(groupId, topic, offsetTracking).map {
      _.foreach(offset => counter(offset.partitionId) = offset.leaseCounter.getOrElse(0))
    }
  }

  override def commit(groupId: String,
                      topic: String,
                      offsetMap: OffsetMap)
                     (implicit executionContext: ExecutionContext): Future[Unit] = {

    val offsetTracking = offsetMap.toCommitRequestInfo(leaseHolder, Some(leaseId), newLeaseTimeout)
    execCommit(groupId, topic, offsetTracking)
  }

  override def isLeaseAvailable(groupId: String, topic: String, partitionId: String)
                               (implicit executionContext: ExecutionContext): Future[Boolean] = {

    commitHandler.get(groupId, topic, partitionId).flatMap {
      _.fold(Future.successful(true)) { offsetTracking =>
        if (offsetTracking.leaseCounter.contains(counter.getOrElse(partitionId, 0))
              && offsetTracking.leaseTimestamp.isAfterNow) {
          execCommit(groupId, topic, Seq(offsetTracking.copy(leaseTimestamp = newLeaseTimeout))).map(_ => true)
        } else Future.successful(false)
      }
    }
  }
}

object LeaseManager {
  def apply(leaseHolder: String,
            commitHandler: BaseCommitHandler,
            staleLeaseDelta: FiniteDuration,
            idGenerator: IdGenerator = IdGenerator): LeaseManager = {
    new LeaseManagerImpl(leaseHolder, commitHandler, staleLeaseDelta, idGenerator)
  }
}
