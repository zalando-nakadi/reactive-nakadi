package de.zalando.react.nakadi

import com.typesafe.config.Config
import de.zalando.react.nakadi.utils.IdGenerator
import de.zalando.react.nakadi.commit.{OffsetMap, OffsetTracking}
import de.zalando.react.nakadi.commit.handlers.{BaseHandler => BaseCommitHandler}
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


trait LeaseManager {

  def leaseId: String

  def partitionId: String

  def leaseHolder: String

  def counter: Long

  def isLeaseAvailable(groupId: String, topic: String)(implicit executionContext: ExecutionContext): Future[Boolean]

  def commit(groupId: String, topic: String, offsetMap: OffsetMap)(implicit executionContext: ExecutionContext): Future[Unit]

}

object LeaseManager {
  def apply(leaseHolder: String,
            partitionId: String,
            commitHandler: BaseCommitHandler,
            config: Config,
            idGenerator: IdGenerator = IdGenerator): LeaseManager = {
    new LeaseManagerImpl(leaseHolder, partitionId, commitHandler, config, idGenerator)
  }
}


class LeaseManagerImpl(override val leaseHolder: String,
                       override val partitionId: String,
                       commitHandler: BaseCommitHandler,
                       config: Config, idGenerator: IdGenerator) extends LeaseManager {

  private var _counter: Long = 0
  private val _leaseId: String = idGenerator.generate
  private val staleLeaseDelta: FiniteDuration = config.getInt("lease-management.stale-lease-delta").seconds  // 10 mins

  def newLeaseTimeout: DateTime = new DateTime(DateTimeZone.UTC).plusSeconds(staleLeaseDelta.length.toInt)
  override def counter: Long = _counter
  override def leaseId: String = _leaseId

  private def execCommit(groupId: String,
                         topic: String,
                         offsetTracking: Seq[OffsetTracking])
                        (implicit executionContext: ExecutionContext): Future[Unit] = {

    commitHandler.put(groupId, topic, offsetTracking).map(_ => _counter += 1)
  }

  override def commit(groupId: String,
                      topic: String,
                      offsetMap: OffsetMap)
                     (implicit executionContext: ExecutionContext): Future[Unit] = {

    val offsetTracking = offsetMap.toCommitRequestInfo(leaseHolder, Some(leaseId), newLeaseTimeout)
    execCommit(groupId, topic, offsetTracking)
  }

  override def isLeaseAvailable(groupId: String, topic: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {

    commitHandler.get(groupId, topic, partitionId).flatMap {
      _.fold(Future.successful(true)) { offsetTracking =>
        if (offsetTracking.leaseCounter.contains(_counter) && offsetTracking.leaseTimestamp.isAfterNow) {
          execCommit(groupId, topic, Seq(offsetTracking.copy(leaseTimestamp = newLeaseTimeout))).map(_ => true)
        } else Future.successful(false)
      }
    }
  }
}
