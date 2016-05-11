package org.zalando.react.nakadi.commit

import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{Matchers, FlatSpec}


class OffsetMapSpec extends FlatSpec with Matchers {

  "OffsetMap" should "return an offset given a partitoin" in {
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    val eventTypePartition2 = EventTypePartition("my-event-type", "10")
    val offset = OffsetMap(Map(eventTypePartition1.hash -> 10, eventTypePartition2.hash -> 0))
    offset.lastOffset(eventTypePartition1) should === (10)
  }

  it should "return -1 if partition not found" in {
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    val eventTypePartition2 = EventTypePartition("my-event-type", "10")
    val offset = OffsetMap(Map(eventTypePartition1.hash -> 10, eventTypePartition2.hash -> 0))
    offset.lastOffset(EventTypePartition("non-exist", "20")) should === (-1L)
  }

  it should "return a difference of two offsets" in {
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    val eventTypePartition2 = EventTypePartition("my-event-type", "10")
    var offset1 = OffsetMap(Map(eventTypePartition1.hash -> 10))
    var offset2 = OffsetMap(Map(eventTypePartition2.hash -> 12))
    offset1 diff offset2 should === (OffsetMap(Map(eventTypePartition1.hash -> 10)))

    offset1 = OffsetMap(Map(eventTypePartition1.hash -> 10, eventTypePartition2.hash -> 2))
    offset2 = OffsetMap(Map(eventTypePartition2.hash -> 12))
    offset1 diff offset2 should === (OffsetMap(Map(eventTypePartition1.hash -> 10, eventTypePartition2.hash -> 2)))

    offset1 = OffsetMap(Map(eventTypePartition2.hash -> 12))
    offset2 = OffsetMap(Map(eventTypePartition1.hash -> 10, eventTypePartition2.hash -> 2))
    offset1 diff offset2 should === (OffsetMap(Map(eventTypePartition2.hash -> 12)))
  }

  it should "return empty offset map if both empty" in {
    OffsetMap() diff OffsetMap() should === (OffsetMap())
  }

  it should "be able to add a new offset and return a new instance" in {
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    val eventTypePartition2 = EventTypePartition("my-event-type", "10")
    val offset = OffsetMap(Map(eventTypePartition1.hash -> 10))
    offset.plusOffset(eventTypePartition2, 20) should === (OffsetMap(Map(eventTypePartition1.hash -> 10, eventTypePartition2.hash -> 20)))
    offset.plusOffset(eventTypePartition2, 30) should === (OffsetMap(Map(eventTypePartition1.hash -> 10, eventTypePartition2.hash -> 30)))
  }

  it should "be able to update an existing offset" in {
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    val offset = OffsetMap(Map(eventTypePartition1.hash -> 10))
    offset.updateWithOffset(eventTypePartition1, 20)
    offset should === (OffsetMap(Map(eventTypePartition1.hash -> 20)))
  }

  it should "correctly identify nonEmpty" in {
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    OffsetMap(Map(eventTypePartition1.hash -> 10)).nonEmpty should === (true)
  }

}
