package de.zalando.react.nakadi

import akka.actor.{ActorRef, ActorLogging, Props}
import akka.stream.actor.{ActorSubscriberMessage, RequestStrategy, ActorSubscriber}

import de.zalando.react.nakadi.NakadiMessages._
import de.zalando.react.nakadi.client.NakadiClientImpl.EventRecord


object NakadiActorSubscriber {

  def props(consumerAndProps: ReactiveNakadiProducer, requestStrategyProvider: () => RequestStrategy) = {
    Props(new NakadiActorSubscriber(consumerAndProps, requestStrategyProvider))
  }
}

class NakadiActorSubscriber(producerAndProps: ReactiveNakadiProducer, requestStrategyProvider: () => RequestStrategy) extends ActorSubscriber
  with ActorLogging {

  override protected val requestStrategy = requestStrategyProvider()
  private val client: ActorRef = producerAndProps.nakadiClient

  override def receive: Receive = {
    case ActorSubscriberMessage.OnNext(element)   => processElement(element.asInstanceOf[StringProducerMessage])
    case ActorSubscriberMessage.OnError(ex)       => handleError(ex)
    case ActorSubscriberMessage.OnComplete        => stop()
  }

  private def processElement(message: StringProducerMessage) = {
    println(s"processElement of the subscriber called with: $message")
    client ! EventRecord(
      events = message.eventRecord,
      flowId = message.flowId
    )
  }

  private def handleError(ex: Throwable) = {
    log.error(ex, "Stopping Kafka subscriber due to fatal error.")
    stop()
  }

  def stop() = {
    context.stop(self)
  }
}
