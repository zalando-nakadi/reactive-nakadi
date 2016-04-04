package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.commit.OffsetTracking
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.Future


object InMemoryCommitHandler extends BaseHandler {

  private val store = scala.collection.concurrent.TrieMap.empty[String, String]
  private def generateKey(group: String, topic: String, partition: String) = s"$group-$topic-$partition"
  private def generateValue(offset: String, leaseHolder: String) = s"$offset-$leaseHolder"

  override def commitSync(groupId: String, topic: String, offsets: Seq[OffsetTracking]): Future[Unit] = {
    Future.successful {
      offsets.foreach { cursor =>
        val key = generateKey(groupId, topic, cursor.partitionId)
        val value = generateValue(cursor.checkpointId, cursor.leaseHolder)
        store.put(key, value)
      }
      println(s"committed offsets: $store")
    }
  }

  override def readCommit(groupId: String, topic: String, partitionId: String): Future[Option[OffsetTracking]] = {
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


