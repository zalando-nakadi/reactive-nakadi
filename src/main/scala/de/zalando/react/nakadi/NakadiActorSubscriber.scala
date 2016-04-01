package de.zalando.react.nakadi

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy}

import de.zalando.react.nakadi.NakadiMessages._


object NakadiActorSubscriber {

  def props(consumerAndProps: ReactiveNakadiProducer, requestStrategyProvider: () => RequestStrategy) = {
    Props(new NakadiActorSubscriber(consumerAndProps, requestStrategyProvider))
  }
}

class NakadiActorSubscriber(producerAndProps: ReactiveNakadiProducer, requestStrategyProvider: () => RequestStrategy)
  extends ActorSubscriber
  with ActorLogging {

  override protected val requestStrategy = requestStrategyProvider()
  private val client: ActorRef = producerAndProps.nakadiClient

  override def receive: Receive = {
    case ActorSubscriberMessage.OnNext(element)   => processElement(element.asInstanceOf[StringProducerMessage])
    case ActorSubscriberMessage.OnError(ex)       => handleError(ex)
    case ActorSubscriberMessage.OnComplete        => stop()
  }

  private def processElement(message: StringProducerMessage) = client ! message

  private def handleError(ex: Throwable) = {
    log.error(ex, "Stopping Nakadi subscriber due to fatal error.")
    stop()
  }

  def stop() = {
    context.stop(self)
  }
}
