package scala.de.zalando.react.nakadi

import de.zalando.react.nakadi.commit.OffsetTracking
import de.zalando.react.nakadi.commit.handlers.BaseLeaseManager
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.Future


object InMemoryCommitLeaseManager extends BaseLeaseManager {

  private val store = scala.collection.concurrent.TrieMap.empty[String, String]
  private def generateKey(group: String, topic: String, partition: String) = s"$group-$topic-$partition"
  private def generateValue(offset: String, leaseHolder: String, leaseCounter: Long) = s"$offset-$leaseHolder-$leaseCounter"

  def create(key: String, partitionId: String, checkpointId: String, leaseHolder: String) = {
    val value = generateValue(checkpointId, leaseHolder, 0)
    store.put(key, value)
    OffsetTracking(
      partitionId = partitionId,
      checkpointId = value.split("-")(0),
      leaseHolder = value.split("-")(1),
      leaseCounter = Option(1),
      leaseTimestamp = new DateTime(DateTimeZone.UTC),
      leaseId = None
    )
  }

  def update(key: String, value: String, partitionId: String) = {
    val count = value.split("-")(2).toLong
    val leaseCounter = count + 1

    val offset = OffsetTracking(
      partitionId = partitionId,
      checkpointId = value.split("-")(0),
      leaseHolder = value.split("-")(1),
      leaseCounter = Option(leaseCounter),
      leaseTimestamp = new DateTime(DateTimeZone.UTC),
      leaseId = None
    )
    store.put(key, generateValue(offset.checkpointId, offset.leaseHolder, count))
    offset
  }

  override def put(groupId: String, topic: String, offset: OffsetTracking): Future[OffsetTracking] = Future.successful {
    val key = generateKey(groupId, topic, offset.partitionId)
    store.get(key)
      .fold(create(key, offset.partitionId, offset.checkpointId, offset.leaseHolder))(update(key, _, offset.partitionId))
  }

  override def get(groupId: String, topic: String, partitionId: String): Future[Option[OffsetTracking]] = {
    Future.successful {
      val key = generateKey(groupId, topic, partitionId)
      store.get(key).map { value =>
        OffsetTracking(
          partitionId = partitionId,
          checkpointId = value.split("-")(0),
          leaseHolder = value.split("-")(1),
          leaseCounter = Option(1),
          leaseTimestamp = new DateTime(DateTimeZone.UTC),
          leaseId = None
        )
      }
    }
  }
}