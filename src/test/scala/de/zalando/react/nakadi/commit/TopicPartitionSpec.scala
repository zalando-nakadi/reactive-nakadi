package de.zalando.react.nakadi.commit

import org.scalatest.{Matchers, FlatSpec}

import de.zalando.react.nakadi.utils.HashService


class TopicPartitionSpec extends FlatSpec with Matchers {

  "TopicPartition" should "generate correct hash from given topic and partition" in {
    // test based off output from org.apache.kafka.common.TopicPartition
    val topicPartition = TopicPartition("my-topic", "15")
    topicPartition.hash should === (HashService.generate("my-topic - 15"))
  }

  it should "match hash code given two different TopicPartition instances" in {
    // test based off output from org.apache.kafka.common.TopicPartition
    val topicPartition1 = TopicPartition("my-topic", "15")
    val topicPartition2 = TopicPartition("my-topic", "15")
    topicPartition1.hash == topicPartition2.hash should === (true)
  }

  it should "not match hash code given two different TopicPartition instances with two different values" in {
    // test based off output from org.apache.kafka.common.TopicPartition
    val topicPartition1 = TopicPartition("my-topic", "15")
    val topicPartition2 = TopicPartition("my-topic", "10")
    topicPartition1.hash == topicPartition2.hash should === (false)
  }

  it should "be able to match two TopicPartitions instances" in {
    val topicPartition1 = TopicPartition("my-topic", "15")
    val topicPartition2 = TopicPartition("my-topic", "15")
    topicPartition1.equals(topicPartition2) should === (true)
  }

  it should "fail to match to match two TopicPartitions instances" in {
    TopicPartition("my-topic", "15").equals("") should === (false)
    TopicPartition("my-topic", "15").equals(None) should === (false)
    TopicPartition("my-topic", "15").equals(1234) should === (false)
    TopicPartition("my-topic", "15").equals(TopicPartition("no-match", "15")) should === (false)
    TopicPartition("my-topic", "15").equals(TopicPartition("my-topic", "35")) should === (false)
  }

}
