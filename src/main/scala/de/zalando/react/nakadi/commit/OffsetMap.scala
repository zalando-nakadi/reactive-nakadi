package de.zalando.react.nakadi.commit

import de.zalando.react.nakadi.NakadiMessages
import de.zalando.react.nakadi.NakadiMessages.Cursor


case class OffsetMap(var map: Offsets = Map.empty) {

  def lastOffset(topicPartition: TopicPartition) = map.getOrElse(topicPartition, -1L)

  def diff(other: OffsetMap) = {
    OffsetMap((map.toSet diff other.map.toSet).toMap)
  }

  def plusOffset(topicPartition: TopicPartition, offset: Offset) = {
    this.copy(map = map + (topicPartition -> offset))
  }

  def updateWithOffset(topicPartition: TopicPartition, offset: Offset) = {
    map = map + (topicPartition -> offset)
  }

  def nonEmpty = map.nonEmpty

  def toCommitRequestInfo: Seq[Cursor] = {

    map.map { values =>
      NakadiMessages.Cursor(
        partition = values._1.partition.toString,
        offset = values._2.toString
      )
    }.toSeq
  }

}

object OffsetMap {

  import de.zalando.react.nakadi.client.models._

  val OffsetMapping = Map(OffsetSymbolicValue.Begin.toString -> 0L)

  def apply() = new OffsetMap()

  def offsetFromString(offset: String): Offset = {
    if (offset.forall(Character.isDigit)) offset.toLong
    else OffsetMapping.getOrElse(offset, throw new IllegalArgumentException("Invalid offset value")).toLong
  }
}
