package de.zalando.react.nakadi

import akka.stream.actor.ActorPublisher
import akka.actor.{ActorLogging, ActorRef, Props}

import de.zalando.react.nakadi.client.providers.ConsumeStatus
import de.zalando.react.nakadi.client.models.EventStreamBatch
import de.zalando.react.nakadi.NakadiMessages.StringConsumerMessage


object NakadiActorPublisher {

  def props(consumerAndProps: ReactiveNakadiConsumer) = {
    Props(new NakadiActorPublisher(consumerAndProps))
  }
}


class NakadiActorPublisher(consumerAndProps: ReactiveNakadiConsumer) extends ActorPublisher[StringConsumerMessage]
  with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._

  private val client: ActorRef = consumerAndProps.nakadiClient

  override def preStart() = client ! ConsumeStatus.Start

  override def receive: Receive = {

    case ConsumeStatus.Init => sender() ! ConsumeStatus.Acknowledge
    case rawEvent: EventStreamBatch if totalDemand > 0 =>
      onNext(toMessage(rawEvent))
      sender() ! ConsumeStatus.Acknowledge
    case SubscriptionTimeoutExceeded  => stop()
    case Cancel                       => stop()
  }

  private def toMessage(rawEvent: EventStreamBatch) = {
    val cursor: Option[NakadiMessages.Cursor] = rawEvent.cursor.map(c => NakadiMessages.Cursor(partition = c.partition, offset = c.offset))
    NakadiMessages.ConsumerMessage(cursor = cursor, events = rawEvent.events.getOrElse(Nil))
  }

  def stop() = context.stop(self)

}
