package de.zalando.react.nakadi

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import de.zalando.react.nakadi.NakadiMessages.{Offset, ConsumerMessage}
import de.zalando.react.nakadi.commit.handlers.aws.DynamoDBHandler

import scala.concurrent.duration._


object TestApp extends App {

  val token = "666ff642-c546-46fa-ae20-2bb1808a59db"

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()

  val publisher: PublisherWithCommitSink = nakadi.consumeWithOffsetSink(ConsumerProperties(
    server = "nakadi-sandbox.aruha-test.zalan.do",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "reactive-nakadi-testing",
    groupId = "some-group",
    partition = "0",
    commitHandler = new DynamoDBHandler(system),
    offset = Some(Offset("300")),
    acceptAnyCertificate = true,
    port = 443,
    urlSchema = "https://"
  ))

  def throttle(msg: ConsumerMessage) = {
    Thread.sleep(1000)
    msg
  }

  def echo(msg: ConsumerMessage) = {
    println(s"From consumer: $msg")
    msg
  }

  Source
    .fromPublisher(publisher.publisher)
    .map(echo)
    .runWith(publisher.offsetCommitSink)
}
