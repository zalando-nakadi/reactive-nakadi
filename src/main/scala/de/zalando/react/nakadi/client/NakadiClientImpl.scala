package de.zalando.react.nakadi.client

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import de.zalando.react.nakadi.client.providers.ConsumeCommand
import de.zalando.react.nakadi.{ConsumerProperties, ProducerProperties}
import de.zalando.react.nakadi.client.providers.{ConsumeEvents, HttpClientProvider, ProduceEvents}

import scala.concurrent.duration.Duration


private[client] case class Properties(
  server: String,
  securedConnection: Boolean,
  tokenProvider: () => String,
  port: Int,
  acceptAnyCertificate: Boolean,
  urlSchema: String,
  connectionTimeout: Duration,
  consumerProperties: Option[ConsumerProperties] = None,
  producerProperties: Option[ProducerProperties] = None
)

object NakadiClientImpl {

  case class EventRecord(events: Seq[models.Event], flowId: Option[ids.FlowId] = None)

  def props(consumerProperties: ConsumerProperties) = {
    val p = Properties(
      server = consumerProperties.server,
      securedConnection = consumerProperties.securedConnection,
      tokenProvider = consumerProperties.tokenProvider,
      port = consumerProperties.port,
      acceptAnyCertificate = consumerProperties.acceptAnyCertificate,
      urlSchema = consumerProperties.urlSchema,
      connectionTimeout = consumerProperties.connectionTimeout,
      consumerProperties = Option(consumerProperties)
    )
    Props(new NakadiClientImpl(p))
  }

  def props(producerProperties: ProducerProperties) = {
    val p = Properties(
      server = producerProperties.server,
      securedConnection = producerProperties.securedConnection,
      tokenProvider = producerProperties.tokenProvider,
      port = producerProperties.port,
      acceptAnyCertificate = producerProperties.acceptAnyCertificate,
      urlSchema = producerProperties.urlSchema,
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

  val clientProvider = new HttpClientProvider(
    context, properties.server, properties.port, properties.acceptAnyCertificate, properties.connectionTimeout
  )

  import NakadiClientImpl._

  override def receive: Receive = {
    case ConsumeCommand.Start => listenForEvents(sender())
    case eventRecord: EventRecord => publishEvent(eventRecord.events, eventRecord.flowId)
  }

  override def publishEvent(events: Seq[models.Event], flowId: Option[ids.FlowId] = None): Unit = {
    val p = properties.producerProperties.getOrElse(sys.error("Producer Properties cannon be None"))
    val produceEvents = new ProduceEvents(p, context, log, clientProvider)
    produceEvents.publish(events, flowId)
  }

  override def listenForEvents(receiverActorRef: ActorRef): Unit = {
    val p = properties.consumerProperties.getOrElse(sys.error("Consumer Properties cannon be None"))
    val consumeEvents = new ConsumeEvents(p, context, log, clientProvider)
    consumeEvents.stream(receiverActorRef)
  }
}
