package de.zalando.react.nakadi


object NakadiMessages {

  type Event = String
  type Topic = String

  case class ProducerMessage(
    eventRecord: Seq[Event],
    flowId: Option[String] = None
  )

  case class Cursor(
    partition: String,
    offset: String
  )

  case class ConsumerMessage(
    topic: Topic,
    events: Seq[Event],
    cursor: Cursor
  )

  type StringConsumerMessage = ConsumerMessage
  type StringProducerMessage = ProducerMessage
}

