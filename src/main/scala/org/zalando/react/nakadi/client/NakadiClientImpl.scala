package org.zalando.react.nakadi.client

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.zalando.react.nakadi.NakadiMessages.{EventTypeMessage, ProducerMessage}
import org.zalando.react.nakadi.client.providers._
import org.zalando.react.nakadi.properties.{ConsumerProperties, ProducerProperties, ServerProperties}
import akka.pattern._
import scala.concurrent.Future


case class Properties(
  serverProperties: ServerProperties,
  tokenProvider: Option[() => String],
  consumerProperties: Option[ConsumerProperties] = None,
  producerProperties: Option[ProducerProperties] = None
)

object NakadiClientImpl {

  def props(consumerProperties: ConsumerProperties) = {
    val p = Properties(
      serverProperties = consumerProperties.serverProperties,
      tokenProvider = consumerProperties.tokenProvider,
      consumerProperties = Option(consumerProperties)
    )
    Props(new NakadiClientImpl(p))
  }

  def props(producerProperties: ProducerProperties) = {
    val p = Properties(
      serverProperties = producerProperties.serverProperties,
      tokenProvider = producerProperties.tokenProvider,
      producerProperties = Option(producerProperties)
    )
    Props(new NakadiClientImpl(p))
  }

  case object MessagePublished
}


class NakadiClientImpl(val properties: Properties) extends Actor
  with ActorLogging
  with NakadiClient {

  import NakadiClientImpl.MessagePublished

  final implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  implicit val ec = context.dispatcher

  val clientProvider = new HttpClientProvider(
    actorContext = context,
    server = properties.serverProperties.host,
    port = properties.serverProperties.port,
    isConnectionSSL = properties.serverProperties.isConnectionSSL,
    acceptAnyCertificate = properties.serverProperties.acceptAnyCertificate,
    connectionTimeout = properties.serverProperties.connectionTimeout
  )

  override def postStop() = clientProvider.http.shutdownAllConnectionPools()

  override def receive: Receive = {
    case ConsumeCommand.Start => listenForEvents(sender())
    case producerMessage: ProducerMessage => publishEvent(producerMessage).map(_ => MessagePublished) pipeTo sender()
    case eventTypeMessage: EventTypeMessage => postEventType(eventTypeMessage: EventTypeMessage)
  }

  override def postEventType(eventTypeMessage: EventTypeMessage): Future[Unit] = {
    val postEvents = new PostEventType(properties, context, log, clientProvider)
    postEvents.post(eventTypeMessage)
  }

  override def publishEvent(producerMessage: ProducerMessage): Future[Unit] = {
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
