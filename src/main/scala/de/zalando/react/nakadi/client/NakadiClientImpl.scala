package de.zalando.react.nakadi.client

import akka.actor.{Actor, ActorLogging, Props}
import akka.stream.scaladsl.ImplicitMaterializer
import de.zalando.react.nakadi.{ProducerProperties, ConsumerProperties}
import de.zalando.react.nakadi.client.providers.{ConsumeEvents, ProduceEvents, HttpProvider}


private[client] case class Properties(
  server: String,
  securedConnection: Boolean,
  tokenProvider: () => String,
  port: Int = 80,
  sslVerify: Boolean = true,
  urlSchema: String = "https://",
  consumerProperties: Option[ConsumerProperties] = None,
  producerProperties: Option[ProducerProperties] = None
)

object NakadiClientImpl {

  case object ListenForEvents
  case class EventRecord(events: Seq[String], flowId: Option[String] = None)

  def props(consumerProperties: ConsumerProperties) = {
    val p = Properties(
      consumerProperties.server,
      consumerProperties.securedConnection,
      consumerProperties.tokenProvider,
      consumerProperties.port,
      consumerProperties.sslVerify,
      consumerProperties.urlSchema,
      consumerProperties = Option(consumerProperties)
    )
    Props(new NakadiClientImpl(p))
  }

  def props(producerProperties: ProducerProperties) = {
    val p = Properties(
      producerProperties.server,
      producerProperties.securedConnection,
      producerProperties.tokenProvider,
      producerProperties.port,
      producerProperties.sslVerify,
      producerProperties.urlSchema,
      producerProperties = Option(producerProperties)
    )
    Props(new NakadiClientImpl(p))
  }
}


class NakadiClientImpl(val properties: Properties) extends Actor
  with ImplicitMaterializer
  with ActorLogging
  with NakadiClient {

  // FIXME - Need general retry mechanism
  val http = new HttpProvider(context,
                              properties.server,
                              properties.port,
                              properties.securedConnection,
                              properties.sslVerify).http

  override def receive: Receive = {
    case NakadiClientImpl.ListenForEvents => listenForEvents()
    case eventRecord: NakadiClientImpl.EventRecord => publishEvent(eventRecord.events, eventRecord.flowId)
  }

  override def publishEvent(events: Seq[String], flowId: Option[String]): Unit = {
    val p = properties.producerProperties.getOrElse(sys.error("Producer Properties cannon be None"))
    val produceEvents = new ProduceEvents(p, http, log, context)
    produceEvents.publish(events, flowId)
  }

  override def listenForEvents(): Unit = {
    val p = properties.consumerProperties.getOrElse(sys.error("Consumer Properties cannon be None"))
    val consumeEvents = new ConsumeEvents(p, http, log, context)
    consumeEvents.stream()
  }
}
