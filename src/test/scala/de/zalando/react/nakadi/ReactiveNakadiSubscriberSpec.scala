package de.zalando.react.nakadi

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSource
import de.zalando.react.nakadi.NakadiMessages.ProducerMessage

import org.scalatest.FlatSpec

class ReactiveNakadiSubscriberSpec extends FlatSpec {

  implicit val actorSystem = ActorSystem("test")
  implicit val materializer = ActorMaterializer()

  "ReactiveNakadiSubscriberSpec" should "cancel upstream if error occurs while sending messages" in {
    val nakadiProducerProperties = ProducerProperties(
      server = "localhost",
      securedConnection = false,
      tokenProvider = () => "",
      topic = "test-topic",
      port = 8080
    )

    val nakadiSubscriber = new ReactiveNakadi().publish(nakadiProducerProperties)

    val sink = Sink.fromSubscriber(nakadiSubscriber)

    val probe = TestSource.probe[String]
      .map(m => ProducerMessage(Seq(m)))
      .toMat(sink)(Keep.left)
      .run()

    // FIXME - Need to implement some kind of supervisor for errors
    //probe.sendNext("dummy").expectCancellation()
  }

}
