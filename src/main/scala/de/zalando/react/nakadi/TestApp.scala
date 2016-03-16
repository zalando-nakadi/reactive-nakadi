package de.zalando.react.nakadi

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.ActorMaterializer

import com.typesafe.config.ConfigFactory
import de.zalando.react.nakadi.NakadiMessages.ConsumerMessage
import de.zalando.react.nakadi.commit.handlers.aws.DynamoDBHandler

import scala.concurrent.duration._


object TestApp extends App {

  val token = ""

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
    sslVerify = false,
    commitHandler = Some(new DynamoDBHandler(system)),
    port = 443,
    urlSchema = "https://",
    commitInterval = Some(10.seconds)
  ))

  def echo(msg: ConsumerMessage) = {
    println(s"From consumer: $msg")
    msg
  }

  Source
    .fromPublisher(publisher.publisher)
    .map(echo)
    .to(publisher.offsetCommitSink)
    .run()

}
