package de.zalando.react.nakadi

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.ActorMaterializer
import org.reactivestreams.{Publisher, Subscriber}
import de.zalando.react.nakadi.commit.handlers.aws.DynamoDBCommitManager
import de.zalando.react.nakadi.NakadiMessages._
import org.joda.time.DateTime
import com.typesafe.config.ConfigFactory
import de.zalando.react.nakadi.properties.{ConsumerProperties, LeaseProperties, ServerProperties}
import play.api.libs.json.Json


object TestApp extends App {

  val tokenVal = "050e4925-fcb4-4e7d-8b1e-b6eee80c1b3c"

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()
  val server = ServerProperties(
    "nakadi-sandbox.aruha-test.zalan.do", port = 443, isConnectionSSL = true)

  val publisher = nakadi.consume(ConsumerProperties(
    serverProperties = server,
    tokenProvider = Option(() => tokenVal),
    eventType = "buffalo-test-topic",
    groupId = "some-group",
    partition = "6",
    commitHandler = DynamoDBCommitManager(system, LeaseProperties.apply),
    offset = Some(BeginOffset)
  ))

  Source
    .fromPublisher(publisher)
//    .map { msg =>
//      Thread.sleep(1000)
//      msg
//    }
    .map(println)
    .to(Sink.ignore)
    .run()
}
