package de.zalando.react.nakadi

import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import de.zalando.react.nakadi.utils.IdGenerator
import de.zalando.react.nakadi.commit.{OffsetMap, OffsetTracking, TopicPartition}
import de.zalando.react.nakadi.commit.handlers.{BaseHandler => BaseCommitHandler}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


trait LeaseManager {

  def leaseId: String

  def leaseHolder: String

  def counter: mutable.Map[String, Long]

  def flush(groupId: String, topic: String, partitionId: String, offsetMap: OffsetMap)(implicit executionContext: ExecutionContext): Future[Boolean]

  def requestLease(groupId: String, topic: String, partitionId: String)(implicit executionContext: ExecutionContext): Future[Boolean]

  def releaseLease(groupId: String, topic: String, partitionId: String)(implicit executionContext: ExecutionContext): Future[Unit]

}

class LeaseManagerImpl(override val leaseHolder: String,
                       commitHandler: BaseCommitHandler,
                       staleLeaseDelta: FiniteDuration,
                       log: Option[LoggingAdapter],
                       idGenerator: IdGenerator = IdGenerator) extends LeaseManager {

  def now = new DateTime(DateTimeZone.UTC)
  def newLeaseTimeout = now.plusSeconds(staleLeaseDelta.length.toInt)

  // Key / value for partition id and lease counter
  override val counter: mutable.Map[String, Long] = mutable.Map.empty
  override val leaseId: String = idGenerator.generate

  // Logging adapter is an optional attribute
  private def maybeWarn(msg: String) = log.foreach(_.warning(msg))

  private def execCommit(groupId: String,
                         topic: String,
                         offsetTracking: OffsetTracking)
                        (implicit executionContext: ExecutionContext): Future[Unit] = {

    commitHandler.put(groupId, topic, offsetTracking).map {
      offset => counter(offset.partitionId) = offset.leaseCounter.getOrElse(0)
    }
  }

  def validate(currentOffset: OffsetTracking): Boolean = {
    val count = counter.getOrElse(currentOffset.partitionId, 0)
    currentOffset.leaseCounter.contains(count) || currentOffset.leaseTimestamp.isBeforeNow
  }

  override def requestLease(groupId: String, topic: String, partitionId: String)
                           (implicit executionContext: ExecutionContext): Future[Boolean] = {
    commitHandler.get(groupId, topic, partitionId).map(_.fold(true)(validate))
  }

  override def releaseLease(groupId: String, topic: String, partitionId: String)
                           (implicit executionContext: ExecutionContext): Future[Unit] = {
    commitHandler.get(groupId, topic, partitionId).map {
      _.fold(maybeWarn(s"No lease exists for group: $groupId topic: $topic partition: $partitionId")) { currentOffset =>
        commitHandler.put(groupId, topic, currentOffset.copy(leaseTimestamp = now, leaseCounter = Option(0)))
      }
    }
  }

  override def flush(groupId: String, topic: String, partitionId: String, offsetMap: OffsetMap)
                    (implicit executionContext: ExecutionContext): Future[Boolean] = {

    val offsetTracking = OffsetTracking(
      partitionId = partitionId,
      checkpointId = offsetMap.lastOffsetAsString(TopicPartition(topic, partitionId)),
      leaseHolder = leaseHolder,
      leaseTimestamp = newLeaseTimeout,
      leaseId = Option(leaseId)
    )

    def create = execCommit(groupId, topic, offsetTracking).map(_ => true)

    def update(currentOffset: OffsetTracking) = {
      if (validate(currentOffset)) execCommit(groupId, topic, offsetTracking).map(_ => true)
      else Future.successful(false)
    }

    commitHandler.get(groupId, topic, partitionId).flatMap(_.fold(create)(update))
  }
}

object LeaseManager {

  def apply(consumerProperties: ConsumerProperties, actorSystem: ActorSystem): ActorRef = {
    val leaseManager = new LeaseManagerImpl(
      consumerProperties.leaseHolder, consumerProperties.commitHandler,
      consumerProperties.staleLeaseDelta, log = Option(actorSystem.log)
    )
    actorSystem.actorOf(LeaseManagerActor.props(leaseManager))
  }

}
