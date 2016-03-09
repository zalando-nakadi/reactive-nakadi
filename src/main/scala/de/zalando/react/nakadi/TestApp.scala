package de.zalando.react.nakadi

import akka.actor.ActorSystem
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl.{Flow, Source, Sink}

import com.typesafe.config.ConfigFactory
import de.zalando.react.nakadi.client.models.EventStreamBatch
import org.reactivestreams.{Publisher, Subscriber}
import de.zalando.react.nakadi.NakadiMessages.{ConsumerMessage, ProducerMessage}
import de.zalando.react.nakadi.commit.handlers.MemoryCommitHandler

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future


object TestApp extends App {

  val token = "e05479fb-e71c-4801-a07f-ab8b2e6ea938"

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()

//  val publisher: Publisher[ConsumerMessage] = nakadi.consume(ConsumerProperties(
//    //server = "nakadi-sandbox.aruha-test.zalan.do",
//    server = "localhost",
//    securedConnection = false,
//    tokenProvider = () => token,
//    topic = "buffalo-test-topic",
//    sslVerify = false,
//    //port = 443
//    port = 8000
//  ))

//  val subscriber: Subscriber[ProducerMessage] = nakadi.publish(ProducerProperties(
//    server = "nakadi-sandbox.aruha-test.zalan.do",
//    securedConnection = true,
//    tokenProvider = () => token,
//    topic = "buffalo-test-topic-uppercase",
//    sslVerify = false,
//    port = 443
//  ))

//  Source
//    .fromPublisher(publisher)
//    .map(m => ProducerMessage(eventRecord = m.events.map(_.toUpperCase())))
//    .to(Sink.fromSubscriber(subscriber))
//    .run()
//  val slowFlow = Flow[ConsumerMessage].mapAsync(1) { x => akka.pattern.after(400.millis, system.scheduler)(Future.successful(x)) }
//  val echo = Flow[ConsumerMessage].map(v => println(s"From publisher: $v"))
//  Source
//    .fromPublisher(publisher)
//    .via(slowFlow)
//    .via(echo)
//    .to(Sink.ignore)
//    .run()

  val publisher: PublisherWithCommitSink = nakadi.consumeWithOffsetSink(ConsumerProperties(
    server = "nakadi-sandbox.aruha-test.zalan.do",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "buffalo-test-topic",
    sslVerify = false,
    commitHandler = Some(new MemoryCommitHandler),
    port = 443,
    urlSchema = "https://"
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
