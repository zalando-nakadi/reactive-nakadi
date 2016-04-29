package de.zalando.react.nakadi.commit

import de.zalando.react.nakadi.utils.HashService

case class TopicPartition(topic: String, partition: String) { self =>
  override def toString = s"$topic - $partition"
  def hash = HashService.generate(this.toString)
}

case class OffsetMap(var map: Map[String, Long] = Map.empty) {

  def lastOffset(topicPartition: TopicPartition) = map.getOrElse(topicPartition.hash, -1L)

  def lastOffsetAsString(topicPartition: TopicPartition) = {
    map.getOrElse(topicPartition.hash, sys.error(
      s"No offset set for given partition ${topicPartition.partition} and topic ${topicPartition.topic}"
    )).toString
  }

  def diff(other: OffsetMap) = {
    OffsetMap((map.toSet diff other.map.toSet).toMap)
  }

  def plusOffset(topicPartition: TopicPartition, offset: Long) = {
    this.copy(map = map + (topicPartition.hash -> offset))
  }

  def updateWithOffset(topicPartition: TopicPartition, offset: Long) = {
    map = map + (topicPartition.hash -> offset)
  }

  def nonEmpty = map.nonEmpty

}

object OffsetMap {

  def apply() = new OffsetMap()
}
