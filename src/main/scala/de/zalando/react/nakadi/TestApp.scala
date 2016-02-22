package de.zalando.react.nakadi

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import com.typesafe.config.ConfigFactory
import de.zalando.react.nakadi.NakadiMessages.{StringConsumerMessage, ProducerMessage}

/**
  * Created by adrakeford on 17/02/2016.
  */
object TestApp extends App {

  val token = "c43cd9bd-0561-4a55-82f7-48e1f564da12"

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()

  val publisher = nakadi.consume(ConsumerProperties(
    server = "nakadi-sandbox.aruha-test.zalan.do",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "buffalo-test-topic",
    sslVerify = false
  ))

  val subscriber = nakadi.publish(ProducerProperties(
    server = "nakadi-sandbox.aruha-test.zalan.do",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "buffalo-test-topic-uppercase",
    sslVerify = false
  ))

  Source
    .fromPublisher(publisher)
    .map(m => ProducerMessage(eventRecord = m.events.map(_.toUpperCase())))
    .to(Sink.fromSubscriber(subscriber))
    .run()
}
