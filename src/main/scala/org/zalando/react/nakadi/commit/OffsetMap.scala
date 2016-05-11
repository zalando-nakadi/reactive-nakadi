package org.zalando.react.nakadi.commit

import org.zalando.react.nakadi.utils.HashService

case class EventTypePartition(eventType: String, partition: String) { self =>
  override def toString = s"$eventType - $partition"
  def hash = HashService.generate(this.toString)
}

case class OffsetMap(var map: Map[String, Long] = Map.empty) {

  def lastOffset(eventTypePartition: EventTypePartition) = map.getOrElse(eventTypePartition.hash, -1L)

  def lastOffsetAsString(eventTypePartition: EventTypePartition) = {
    map.getOrElse(eventTypePartition.hash, sys.error(
      s"No offset set for given partition ${eventTypePartition.partition} and event-type ${eventTypePartition.eventType}"
    )).toString
  }

  def diff(other: OffsetMap) = {
    OffsetMap((map.toSet diff other.map.toSet).toMap)
  }

  def plusOffset(eventTypePartition: EventTypePartition, offset: Long) = {
    this.copy(map = map + (eventTypePartition.hash -> offset))
  }

  def updateWithOffset(eventTypePartition: EventTypePartition, offset: Long) = {
    map = map + (eventTypePartition.hash -> offset)
  }

  def nonEmpty = map.nonEmpty

}

object OffsetMap {

  def apply() = new OffsetMap()
}
