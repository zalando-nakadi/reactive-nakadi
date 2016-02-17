package de.zalando.react.nakadi

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory

/**
  * Created by adrakeford on 17/02/2016.
  */
object TestApp extends App {

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()
  val publisher = nakadi.consume(ConsumerProperties(
    //server = "192.168.99.100",
    server = "nakadi-test.aurora.zalando.net",
    port = 443,
    securedConnection = true,
    tokenProvider = () => "95346dac-fca9-4b45-b268-83d5bf815e3b",
    topic = "test-topic"
  ))

  Source.fromPublisher(publisher).to(Sink.ignore)
}
