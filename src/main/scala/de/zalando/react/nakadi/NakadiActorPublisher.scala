package de.zalando.react.nakadi

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.stream.actor.{ActorPublisherMessage, ActorPublisher}

import de.zalando.react.nakadi.client.NakadiClientImpl
import de.zalando.react.nakadi.client.models.EventStreamBatch
import de.zalando.react.nakadi.NakadiMessages.StringConsumerMessage


object NakadiActorPublisher {

  def props(consumerAndProps: ReactiveNakadiConsumer) = {
    Props(new NakadiActorPublisher(consumerAndProps))
  }
}


class NakadiActorPublisher(consumerAndProps: ReactiveNakadiConsumer) extends ActorPublisher[StringConsumerMessage]
  with ActorLogging {

  private val client: ActorRef = consumerAndProps.nakadiClient

  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[EventStreamBatch])
    client ! NakadiClientImpl.ListenForEvents
  }

  override def receive: Receive = {
    case rawEvent: EventStreamBatch                         => handleIncoming(rawEvent)
    case ActorPublisherMessage.SubscriptionTimeoutExceeded  => stop()
    case ActorPublisherMessage.Cancel                       => stop()
  }

  private def handleIncoming(rawEvent: EventStreamBatch) = {
    if (isActive && totalDemand > 0) {
      val cursor: Option[NakadiMessages.Cursor] = rawEvent.cursor.map(c => NakadiMessages.Cursor(partition = c.partition, offset = c.offset))
      val consumerMessage = NakadiMessages.ConsumerMessage(cursor = cursor, events = rawEvent.events.getOrElse(Nil))
      onNext(consumerMessage)
    }
  }

  override def postStop() = {
    context.system.eventStream.unsubscribe(self)
  }

  def stop() = {
    context.stop(self)
  }

}
