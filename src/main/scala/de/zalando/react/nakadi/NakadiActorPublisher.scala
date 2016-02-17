package de.zalando.react.nakadi

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.stream.actor.{ActorPublisherMessage, ActorPublisher}


object NakadiActorPublisher {
  case object Consume
  case object Stop

  def props(consumerAndProps: ReactiveNakadiConsumer) = {
    Props(new NakadiActorPublisher(consumerAndProps))
  }
}


class NakadiActorPublisher(consumerAndProps: ReactiveNakadiConsumer) extends ActorPublisher[String]
  with ActorLogging {

  import NakadiActorPublisher._

  val client: ActorRef = consumerAndProps.nakadiClient

  override def preStart() {
    self ! Consume
  }

  override def receive = {
    case ActorPublisherMessage.Request(_) | Consume if isActive => readDemandedItems()
    case ActorPublisherMessage.SubscriptionTimeoutExceeded => context.stop(self)
  }

  private def readDemandedItems(): Unit = {
    client ! NakadiClientImpl.ListenForEvents
  }

}

//object NakadiMessages {
//  type StringConsumerRecord = models.NakadiTypes.Event
//  type StringProducerMessage = models.NakadiTypes.Event
//}
