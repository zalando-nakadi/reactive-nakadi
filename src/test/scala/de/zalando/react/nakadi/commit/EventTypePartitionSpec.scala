package de.zalando.react.nakadi.commit

import org.scalatest.{Matchers, FlatSpec}

import de.zalando.react.nakadi.utils.HashService


class EventTypePartitionSpec extends FlatSpec with Matchers {

  "EventTypePartition" should "generate correct hash from given event-type and partition" in {
    // test based off output from org.apache.kafka.common.TopicPartition
    val eventTypePartition = EventTypePartition("my-event-type", "15")
    eventTypePartition.hash should === (HashService.generate("my-event-type - 15"))
  }

  it should "match hash code given two different EventTypePartition instances" in {
    // test based off output from org.apache.kafka.common.TopicPartition
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    val eventTypePartition2 = EventTypePartition("my-event-type", "15")
    eventTypePartition1.hash == eventTypePartition2.hash should === (true)
  }

  it should "not match hash code given two different EventTypePartition instances with two different values" in {
    // test based off output from org.apache.kafka.common.TopicPartition
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    val eventTypePartition2 = EventTypePartition("my-event-type", "10")
    eventTypePartition1.hash == eventTypePartition2.hash should === (false)
  }

  it should "be able to match two EventTypePartitions instances" in {
    val eventTypePartition1 = EventTypePartition("my-event-type", "15")
    val eventTypePartition2 = EventTypePartition("my-event-type", "15")
    eventTypePartition1.equals(eventTypePartition2) should === (true)
  }

  it should "fail to match to match two EventTypePartitions instances" in {
    EventTypePartition("my-event-type", "15").equals("") should === (false)
    EventTypePartition("my-event-type", "15").equals(None) should === (false)
    EventTypePartition("my-event-type", "15").equals(1234) should === (false)
    EventTypePartition("my-event-type", "15").equals(EventTypePartition("no-match", "15")) should === (false)
    EventTypePartition("my-event-type", "15").equals(EventTypePartition("my-event-type", "35")) should === (false)
  }

}
