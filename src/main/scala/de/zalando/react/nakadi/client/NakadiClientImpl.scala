package de.zalando.react.nakadi.client

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import de.zalando.react.nakadi.NakadiMessages.ProducerMessage
import de.zalando.react.nakadi.client.providers.ConsumeCommand
import de.zalando.react.nakadi.{ConsumerProperties, ProducerProperties}
import de.zalando.react.nakadi.client.providers.{ConsumeEvents, HttpClientProvider, ProduceEvents}

import scala.concurrent.Future
import scala.concurrent.duration.Duration


private[client] case class Properties(
  server: String,
  tokenProvider: Option[() => String],
  acceptAnyCertificate: Boolean,
  connectionTimeout: Duration,
  consumerProperties: Option[ConsumerProperties] = None,
  producerProperties: Option[ProducerProperties] = None
)

object NakadiClientImpl {

  def props(consumerProperties: ConsumerProperties) = {
    val p = Properties(
      server = consumerProperties.server,
      tokenProvider = consumerProperties.tokenProvider,
      acceptAnyCertificate = consumerProperties.acceptAnyCertificate,
      connectionTimeout = consumerProperties.connectionTimeout,
      consumerProperties = Option(consumerProperties)
    )
    Props(new NakadiClientImpl(p))
  }

  def props(producerProperties: ProducerProperties) = {
    val p = Properties(
      server = producerProperties.server,
      tokenProvider = producerProperties.tokenProvider,
      acceptAnyCertificate = producerProperties.acceptAnyCertificate,
      connectionTimeout = producerProperties.connectionTimeout,
      producerProperties = Option(producerProperties)
    )
    Props(new NakadiClientImpl(p))
  }
}


class NakadiClientImpl(val properties: Properties) extends Actor
  with ActorLogging
  with NakadiClient {

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
  val clientProvider = new HttpClientProvider(properties.connectionTimeout, properties.acceptAnyCertificate)

  override def receive: Receive = {
    case ConsumeCommand.Start => listenForEvents(sender())
    case producerMessage: ProducerMessage => publishEvent(producerMessage)
  }

  override def publishEvent(producerMessage: ProducerMessage): Future[Boolean] = {
    val p = properties.producerProperties.getOrElse(sys.error("Producer Properties cannon be None"))
    val produceEvents = new ProduceEvents(p, context, log, clientProvider)
    produceEvents.publish(producerMessage)
  }

  override def listenForEvents(receiverActorRef: ActorRef): Unit = {
    val p = properties.consumerProperties.getOrElse(sys.error("Consumer Properties cannon be None"))
    val consumeEvents = new ConsumeEvents(p, context, log, clientProvider)
    consumeEvents.stream(receiverActorRef)
  }
}
