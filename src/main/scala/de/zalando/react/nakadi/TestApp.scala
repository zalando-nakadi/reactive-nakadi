package de.zalando.react.nakadi

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.ActorMaterializer
import org.reactivestreams.{Publisher, Subscriber}
import de.zalando.react.nakadi.commit.handlers.aws.DynamoDBHandler
import de.zalando.react.nakadi.NakadiMessages._
import org.joda.time.DateTime
import com.typesafe.config.ConfigFactory
import play.api.libs.json.Json


object TestApp extends App {

  def token() = ""

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()

  val publisher = nakadi.consumeWithOffsetSink(ConsumerProperties(
    server = "http://192.168.99.100:8080/",
    tokenProvider = None,
    topic = "reactive-nakadi-testing",
    groupId = "some-group",
    partition = "0",
    commitHandler = new DynamoDBHandler(system),
    //offset = Some(BeginOffset),
    acceptAnyCertificate = true
  ))

  Source
    .fromPublisher(publisher.publisher)
    .to(publisher.offsetCommitSink)
    .run()
}
