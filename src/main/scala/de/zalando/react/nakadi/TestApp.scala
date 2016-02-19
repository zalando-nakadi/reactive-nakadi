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
    server = "nakadi-sandbox.aurora.zalando.net",
    //port = 8080,
    securedConnection = true,
    tokenProvider = () => "e3c8f2c3-101a-42c1-8d02-f82d1a5c8807",
    topic = "buffalo-test-topic",
    sslVerify = false
  ))

  Source.fromPublisher(publisher).to(Sink.foreach(x => println(s"END: $x")))
}
