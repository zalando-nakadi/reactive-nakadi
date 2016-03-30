package de.zalando.react.nakadi


object NakadiMessages {

  type Topic = String

  case class Offset(value: String) {

    override def toString: String = value

    def toLong: Long = {
      if (value.forall(Character.isDigit)) value.toLong
      else if (value.equals(BeginOffset.toString)) 0L
      else throw new IllegalArgumentException("Invalid offset value")
    }
  }

  object BeginOffset extends Offset("BEGIN")

  case class ProducerMessage(
    eventRecord: Seq[String],
    flowId: Option[String] = None
  )

  case class Cursor(
    partition: String,
    offset: Offset
  )

  case class ConsumerMessage(
    topic: Topic,
    events: Seq[String],
    cursor: Cursor
  )

  type StringConsumerMessage = ConsumerMessage
  type StringProducerMessage = ProducerMessage

}

