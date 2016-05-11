package org.zalando.react.nakadi

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.ActorMaterializer
import org.reactivestreams.{Publisher, Subscriber}
import org.zalando.react.nakadi.commit.handlers.aws.DynamoDBCommitManager
import org.joda.time.DateTime
import com.typesafe.config.ConfigFactory
import org.zalando.react.nakadi.properties._
import play.api.libs.json.Json

import scala.concurrent.Future


object TestApp extends App {

  val tokenVal = ""

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()
  val server = ServerProperties("192.168.99.100", port = 8080, isConnectionSSL = false)

  val publisher = nakadi.consumeWithOffsetSink(ConsumerProperties(
    serverProperties = server,
    tokenProvider = None,
    eventType = "reactive-nakadi-testing",
    groupId = "oxygen-buffalo",
    partition = "0",
    commitHandler = DynamoDBCommitManager(system, CommitProperties.apply)
    //offset = Some(BeginOffset)
  ))

  Source
    .fromPublisher(publisher.publisher)
    .map { msg => println(msg); msg }
    .to(publisher.offsetCommitSink)
    .run()
}
