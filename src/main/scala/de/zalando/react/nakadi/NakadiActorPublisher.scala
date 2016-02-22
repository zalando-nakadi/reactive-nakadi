package de.zalando.react.nakadi

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.stream.actor.{ActorPublisherMessage, ActorPublisher}

import de.zalando.react.nakadi.client.NakadiClientImpl
import de.zalando.react.nakadi.client.models.EventStreamBatch


object NakadiActorPublisher {
  case object Consume
  case object Stop

  def props(consumerAndProps: ReactiveNakadiConsumer) = {
    Props(new NakadiActorPublisher(consumerAndProps))
  }
}


class NakadiActorPublisher(consumerAndProps: ReactiveNakadiConsumer) extends ActorPublisher[EventStreamBatch]
  with ActorLogging {

  val client: ActorRef = consumerAndProps.nakadiClient

  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[EventStreamBatch])
    client ! NakadiClientImpl.ListenForEvents
  }

  override def receive = {
    case rawEvent: EventStreamBatch if isActive && totalDemand > 0 => onNext(rawEvent)
    case ActorPublisherMessage.SubscriptionTimeoutExceeded => context.stop(self)
  }

  override def postStop() = {
    context.system.eventStream.unsubscribe(self)
  }

}

//object NakadiMessages {
//  type StringConsumerRecord = models.NakadiTypes.Event
//  type StringProducerMessage = models.NakadiTypes.Event
//}
