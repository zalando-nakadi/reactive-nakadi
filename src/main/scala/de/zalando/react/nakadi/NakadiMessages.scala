package de.zalando.react.nakadi


object NakadiMessages {

  type Event = String

  case class ProducerMessage(
    eventRecord: Seq[Event],
    flowId: Option[String] = None
  )

  case class Cursor(
    partition: String,
    offset: String
  )

  case class ConsumerMessage(
    cursor: Option[Cursor] = None,
    events: Seq[Event]
  )

  type StringConsumerMessage = ConsumerMessage
  type StringProducerMessage = ProducerMessage
}

