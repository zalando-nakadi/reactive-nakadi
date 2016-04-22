package de.zalando.react.nakadi

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSource
import de.zalando.react.nakadi.NakadiMessages.{ConsumerMessage, Cursor, Offset, ProducerMessage}
import de.zalando.react.nakadi.client.models.{DataOpEnum, Event, EventPayload, MetaData}
import org.joda.time.DateTime
import play.api.libs.json.Json

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._


class ReactiveNakadiSubscriberSpec extends NakadiTest {

  override val dockerProvider: DockerProvider = new NakadiDockerProvider(config)
  override implicit val system: ActorSystem = ActorSystem("ReactiveNakadiSubscriberSpec")

  def producerProperties = createProducerProperties
  def consumerProperties = createConsumerProperties
  def generateEvent = {
    Event(
      data_type = "test_data_type",
      data_op = DataOpEnum.C,
      data = Json.parse(s"""{"foo": "bar"}""").as[EventPayload],
      MetaData(
        eid = UUID.randomUUID().toString,
        occurred_at = new DateTime(),
        flow_id = Option("my-test-flow-id")
      )
    )
  }

  def validateEvent(event1: Event, event2: Event): Unit = {
    event1.data_op should === (event2.data_op)
    event1.data_type should === (event2.data_type)
    event1.data should === (event2.data)
    event1.metadata.flow_id should === (event2.metadata.flow_id)
    event1.metadata.eid should === (event2.metadata.eid)
    event1.metadata.event_type should === (Some(topic))
  }

  "ReactiveNakadiSubscriber" should "consume a single messages" in {
    val event = generateEvent
    val nakadiPublisher = nakadi.consume(
      consumerProperties.copy(batchFlushTimeoutInSeconds = 10.seconds, streamTimeoutInSeconds = 10.seconds)
    )
    val resultFut = Source.fromPublisher(nakadiPublisher).runWith(Sink.head)

    val nakadiSubscriber = nakadi.publish(producerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)

    TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()
      .sendNext(Seq(event))

    val result = Await.result(resultFut, 30.seconds)
    println("validate single messages")
    validateEvent(result.events.head, event)
    result.topic should === (topic)
    result.cursor should === (Cursor(partition = "0", offset = Offset(value = "0")))
  }

  it should "consume multiple messages as separate events" in {
    val events = (1 to 10).map(_ => generateEvent)

    val nakadiPublisher = nakadi.consume(
      consumerProperties.copy(batchFlushTimeoutInSeconds = 10.seconds, streamTimeoutInSeconds = 10.seconds)
    )
    Source.fromPublisher(nakadiPublisher).runForeach { v =>
      println(v)
    }

    val nakadiSubscriber = nakadi.publish(producerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)

    val probe = TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    events.foreach(ev => probe.sendNext(Seq(ev)))
  }
}
