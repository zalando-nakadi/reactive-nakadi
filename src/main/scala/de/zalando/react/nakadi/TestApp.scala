package de.zalando.react.nakadi

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
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
    server = "nakadi-sandbox.aruha-test.zalan.do",
    //port = 8080,
    securedConnection = true,
    tokenProvider = () => "ac516b4f-1338-43e6-a6c3-2e98ac8e065e",
    topic = "buffalo-test-topic",
    sslVerify = false
  ))

  Source.fromPublisher(publisher)
    .map(x => println(s"MAP $x"))
    .to(Sink.foreach(x => println(s"END: $x")))
}
