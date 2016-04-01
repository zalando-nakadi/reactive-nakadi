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

  val publisher: Publisher[ConsumerMessage] = nakadi.consume(ConsumerProperties(
    server = "nakadi-sandbox.aruha-test.zalan.do",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "reactive-nakadi-testing",
    groupId = "some-group",
    partition = "0",
    commitHandler = new DynamoDBHandler(system),
    offset = Some(Offset("333")),
    acceptAnyCertificate = true,
    port = 443,
    urlSchema = "https://"
  ))

  val subscriber: Subscriber[ProducerMessage] = nakadi.publish(ProducerProperties(
    server = "nakadi-sandbox.aruha-test.zalan.do",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "reactive-nakadi-testing-uppercase",
    acceptAnyCertificate = true,
    port = 443
  ))

  def makeUpper(msg: StringConsumerMessage): ProducerMessage = {
    import de.zalando.react.nakadi.client.models.{DataOpEnum, Event, MetaData, EventPayload}

    println(s"Incoming message: $msg")
    ProducerMessage(msg.events.map(_.data.toString().toUpperCase).map { rawEvent =>
      Event(
        data_type = "test_data",
        data_op = DataOpEnum.C,
        data = Json.parse(s"""{"foo": "MY_UPPERCASE_VALUE"}""").as[EventPayload],
        MetaData(
          eid = UUID.randomUUID().toString,
          occurred_at = new DateTime(),
          flow_id = Option("my-test-flow-id")
        )
      )
    })
  }

  def echo(msg: ConsumerMessage) = {
    println(s"---: $msg")
    msg
  }

  Source
    .fromPublisher(publisher)
//    .map(makeUpper)
    .map(echo)
    //.to(Sink.fromSubscriber(subscriber))
    .to(Sink.ignore)
    .run()


//  def testingCommits: Unit = {
//    val publisher: PublisherWithCommitSink = nakadi.consumeWithOffsetSink(ConsumerProperties(
//      server = "nakadi-sandbox.aruha-test.zalan.do",
//      securedConnection = true,
//      tokenProvider = () => token,
//      topic = "reactive-nakadi-testing",
//      groupId = "some-group",
//      partition = "0",
//      commitHandler = new DynamoDBHandler(system),
//      acceptAnyCertificate = true,
//      port = 443,
//      urlSchema = "https://"
//    ))
//
//    def throttle(msg: ConsumerMessage) = {
//      Thread.sleep(1000)
//      msg
//    }
//
//    def echo(msg: ConsumerMessage) = {
//      println(s"From consumer: $msg")
//      msg
//    }
//
//    Source
//      .fromPublisher(publisher.publisher)
//      .map(echo)
//      .runWith(publisher.offsetCommitSink)
//  }
}
