package de.zalando.react.nakadi

import play.api.libs.json.Json

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, RequestStrategy}

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
    import de.zalando.react.nakadi.client.ids
    import de.zalando.react.nakadi.client.models
    import de.zalando.react.nakadi.client.ops.Id._

    val record = EventRecord(
      events = message.eventRecord.map(Json.parse(_).asInstanceOf[models.Event]),
      flowId = message.flowId.map(_.id[ids.FlowRef])
    )

    client ! record
  }

  private def handleError(ex: Throwable) = {
    log.error(ex, "Stopping Nakadi subscriber due to fatal error.")
    stop()
  }

  def stop() = {
    context.stop(self)
  }
}
