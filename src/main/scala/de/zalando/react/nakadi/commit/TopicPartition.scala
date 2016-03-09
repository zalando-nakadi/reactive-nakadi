package de.zalando.react.nakadi.commit


case class TopicPartition(topic: String, partition: Int) { self =>

  private var hash: Int = 0

  /**
    * Logic taken from org.apache.kafka.common
    * This allows you to create a hash that is
    * unique for a given partition and topic
    * @return int for hash code
    */
  override def hashCode = {
    if (hash != 0) hash
    else {
      val prime = 31
      var result = 1
      result = prime * result + partition
      result = prime * result + topic.hashCode
      hash = result
      result
    }
  }

  override def toString = s"$topic - $partition"

}
