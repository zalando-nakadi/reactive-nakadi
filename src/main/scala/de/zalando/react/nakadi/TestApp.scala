package de.zalando.react.nakadi

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import com.typesafe.config.ConfigFactory
import de.zalando.react.nakadi.NakadiMessages.ProducerMessage

/**
  * Created by adrakeford on 17/02/2016.
  */
object TestApp extends App {

  val token = "a042e491-2c4f-469f-9f83-baf0f88e9232"

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()

  val publisher = nakadi.consume(ConsumerProperties(
    server = "nakadi-sandbox.aruha-test.zalan.do",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "buffalo-test-topic",
    sslVerify = false,
    port = 443
  ))

  val subscriber = nakadi.publish(ProducerProperties(
    server = "nakadi-sandbox.aruha-test.zalan.do",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "buffalo-test-topic-uppercase",
    sslVerify = false,
    port = 443
  ))

//  Source
//    .fromPublisher(publisher)
//    .map(m => ProducerMessage(eventRecord = m.events.map(_.toUpperCase())))
//    .to(Sink.fromSubscriber(subscriber))
//    .run()

  Source
    .fromPublisher(publisher)
    .map { m =>
      println(s"From publisher: $m")
      //Thread sleep 1000
    }
    .to(Sink.ignore)
    .run()
}
