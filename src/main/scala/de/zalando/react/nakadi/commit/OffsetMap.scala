package de.zalando.react.nakadi.commit

import org.joda.time.{DateTimeZone, DateTime}


case class OffsetMap(var map: Map[TopicPartition, Long] = Map.empty) {

  def lastOffset(topicPartition: TopicPartition) = map.getOrElse(topicPartition, -1L)

  def diff(other: OffsetMap) = {
    OffsetMap((map.toSet diff other.map.toSet).toMap)
  }

  def plusOffset(topicPartition: TopicPartition, offset: Long) = {
    this.copy(map = map + (topicPartition -> offset))
  }

  def updateWithOffset(topicPartition: TopicPartition, offset: Long) = {
    map = map + (topicPartition -> offset)
  }

  def nonEmpty = map.nonEmpty

  def toCommitRequestInfo(leaseHolder: String, leaseId: Option[String]): Seq[OffsetTracking] = {

    map.map { values =>
      OffsetTracking(
        partitionId = values._1.partition.toString,
        checkpointId = values._2.toString,
        leaseHolder = leaseHolder,
        leaseTimestamp = new DateTime(DateTimeZone.UTC),
        leaseId = leaseId
      )
    }.toSeq
  }

}

object OffsetMap {

  import de.zalando.react.nakadi.client.models._

  val OffsetMapping = Map(OffsetSymbolicValue.Begin.toString -> 0L)

  def apply() = new OffsetMap()

  def offsetFromStringToLong(offset: String): Long = {
    if (offset.forall(Character.isDigit)) offset.toLong
    else OffsetMapping.getOrElse(offset, throw new IllegalArgumentException("Invalid offset value")).toLong
  }
}
