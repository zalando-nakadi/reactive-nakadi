package de.zalando.react.nakadi.commit

import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{Matchers, FlatSpec}


class OffsetMapSpec extends FlatSpec with Matchers {

  "OffsetMap" should "return an offset given a partitoin" in {
    val topicPartition1 = TopicPartition("my-topic", 15)
    val topicPartition2 = TopicPartition("my-topic", 10)
    val offset = OffsetMap(Map(topicPartition1 -> 10, topicPartition2 -> 0))
    offset.lastOffset(topicPartition1) should === (10)
  }

  it should "return -1 if partition not found" in {
    val topicPartition1 = TopicPartition("my-topic", 15)
    val topicPartition2 = TopicPartition("my-topic", 10)
    val offset = OffsetMap(Map(topicPartition1 -> 10, topicPartition2 -> 0))
    offset.lastOffset(TopicPartition("non-exist", 20)) should === (-1L)
  }

  it should "return a difference of two offsets" in {
    val topicPartition1 = TopicPartition("my-topic", 15)
    val topicPartition2 = TopicPartition("my-topic", 10)
    var offset1 = OffsetMap(Map(topicPartition1 -> 10))
    var offset2 = OffsetMap(Map(topicPartition2 -> 12))
    offset1 diff offset2 should === (OffsetMap(Map(topicPartition1 -> 10)))

    offset1 = OffsetMap(Map(topicPartition1 -> 10, topicPartition2 -> 2))
    offset2 = OffsetMap(Map(topicPartition2 -> 12))
    offset1 diff offset2 should === (OffsetMap(Map(topicPartition1 -> 10, topicPartition2 -> 2)))

    offset1 = OffsetMap(Map(topicPartition2 -> 12))
    offset2 = OffsetMap(Map(topicPartition1 -> 10, topicPartition2 -> 2))
    offset1 diff offset2 should === (OffsetMap(Map(topicPartition2 -> 12)))
  }

  it should "return empty offset map if both empty" in {
    OffsetMap() diff OffsetMap() should === (OffsetMap())
  }

  it should "be able to add a new offset and return a new instance" in {
    val topicPartition1 = TopicPartition("my-topic", 15)
    val topicPartition2 = TopicPartition("my-topic", 10)
    val offset = OffsetMap(Map(topicPartition1 -> 10))
    offset.plusOffset(topicPartition2, 20) should === (OffsetMap(Map(topicPartition1 -> 10, topicPartition2 -> 20)))
    offset.plusOffset(topicPartition2, 30) should === (OffsetMap(Map(topicPartition1 -> 10, topicPartition2 -> 30)))
  }

  it should "be able to update an existing offset" in {
    val topicPartition1 = TopicPartition("my-topic", 15)
    val offset = OffsetMap(Map(topicPartition1 -> 10))
    offset.updateWithOffset(topicPartition1, 20)
    offset should === (OffsetMap(Map(topicPartition1 -> 20)))
  }

  it should "correctly identify nonEmpty" in {
    val topicPartition1 = TopicPartition("my-topic", 15)
    OffsetMap(Map(topicPartition1 -> 10)).nonEmpty should === (true)
  }

  it should "be able to convert toCommitRequestInfo" in {
    val now = new DateTime(DateTimeZone.UTC)

    val topicPartition1 = TopicPartition("my-topic", 60)
    val topicPartition2 = TopicPartition("my-topic", 50)
    val offset = OffsetMap(Map(topicPartition1 -> 10, topicPartition2 -> 20))
    val expected = Seq(
      OffsetTracking(
        partitionId = "60", checkpointId = "10", leaseHolder = "test-lease-holder",
        leaseTimestamp = now, leaseId = Option("test-lease-id")
      ),
      OffsetTracking(
        partitionId = "50", checkpointId = "20", leaseHolder = "test-lease-holder",
        leaseTimestamp = now, leaseId = Option("test-lease-id")
      )
    )
    offset.toCommitRequestInfo("test-lease-holder", Some("test-lease-id"), now) should === (expected)
  }

}
