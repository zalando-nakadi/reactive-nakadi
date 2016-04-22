package de.zalando.react.nakadi

import de.zalando.react.nakadi.client.models.{EventType, Event}


object NakadiMessages {

  case class Offset(value: String) {

    override def toString: String = value

    def toLong: Long = {
      if (value.forall(Character.isDigit)) value.toLong
      else if (value.equals(BeginOffset.toString)) 0L
      else throw new IllegalArgumentException("Invalid offset value")
    }
  }

  object BeginOffset extends Offset("BEGIN")

  case class ProducerMessage(eventRecords: Seq[Event])
  case class EventTypeMessage(eventType: EventType)

  case class Cursor(
    partition: String,
    offset: Offset
  )

  case class ConsumerMessage(
    topic: String,
    events: Seq[Event],
    cursor: Cursor
  )

  type StringConsumerMessage = ConsumerMessage
  type StringProducerMessage = ProducerMessage

}

