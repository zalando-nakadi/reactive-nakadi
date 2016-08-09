package org.zalando.react.nakadi

import java.time.OffsetDateTime
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import play.api.libs.json.Json
import org.zalando.react.nakadi.client.models._
import org.zalando.react.nakadi.NakadiMessages.{ConsumerMessage, Cursor, Offset, ProducerMessage}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.immutable.Seq


class ReactiveNakadiSubscriberSpec extends NakadiTest {

  override val dockerProvider: DockerProvider = new NakadiDockerProvider
  override implicit val system: ActorSystem = ActorSystem("ReactiveNakadiSubscriberSpec")

  def generateEvent = {
    DataChangeEvent(
      data_type = "test_data_type",
      data_op = DataOpEnum.C,
      data = Json.parse(s"""{"foo": "bar"}""").as[EventPayload],
      EventMetadata(
        eid = UUID.randomUUID().toString,
        occurred_at = OffsetDateTime.now,
        flow_id = Option("my-test-flow-id")
      )
    )
  }

  def validateEvent(event1: Event, event2: Event): Unit = {

    def validateDataEvent(event1: DataChangeEvent, event2: DataChangeEvent) = {
      event1.data_op should === (event2.data_op)
      event1.data_type should === (event2.data_type)
      event1.data should === (event2.data)
      event1.metadata.flow_id should === (event2.metadata.flow_id)
      event1.metadata.eid should === (event2.metadata.eid)
      event1.metadata.event_type should === (Some(eventType))
    }

    def validateBusinessEvent(event1: BusinessEvent, event2: BusinessEvent) = {
      event1.payload should === (event2.payload)
      event1.metadata.flow_id should === (event2.metadata.flow_id)
      event1.metadata.eid should === (event2.metadata.eid)
      event1.metadata.event_type should === (Some(eventType))
    }

    (event1, event2) match {
      case (b1: BusinessEvent, b2: BusinessEvent) => validateBusinessEvent(b1, b2)
      case (d1: DataChangeEvent, d2: DataChangeEvent) => validateDataEvent(d1, d2)
      case _ => fail("event are not the same")
    }


  }

  "Reactive-Nakadi Subscriber" should "consume a single messages" in {
    val event = generateEvent

    // Create the publisher (read from Nakadi stream)
    val nakadiPublisher = nakadi.consume(createConsumerProperties)
    val resultFut = Source.fromPublisher(nakadiPublisher).runWith(Sink.head)

    // Create the subscriber (write to Nakadi)
    val nakadiSubscriber = nakadi.publish(createProducerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)
    TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()
      .sendNext(Seq(event))

    val result = Await.result(resultFut, 30.seconds)
    validateEvent(result.events.head, event)
    result.eventType should === (eventType)
    result.cursor should === (Cursor(partition = "0", offset = Offset("0")))
  }

  it should "consume multiple messages as separate events" in {

    // Create the publisher (read from Nakadi stream)
    val nakadiPublisher = nakadi.consume(createConsumerProperties.copy(offset = Some(Offset("0"))))
    val resultFut = Source
      .fromPublisher(nakadiPublisher)
      .via(Flow[ConsumerMessage].take(10))
      .runWith(Sink.seq)

    // Create the subscriber (write to Nakadi)
    val nakadiSubscriber = nakadi.publish(createProducerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)
    val probe = TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    val events = (1 to 10).map { _ =>
      val event = generateEvent
      probe.sendNext(Seq(event))
      event.metadata.eid -> event
    }.toMap

    val result = Await.result(resultFut, 30.seconds)
    result.zip(Stream from 1).foreach {
      case (res, idx) =>
        res.eventType should === (eventType)
        res.cursor should === (Cursor(partition = "0", offset = Offset(idx.toString)))
        validateEvent(res.events.head, events(res.events.head.metadata.eid))
    }
  }

  it should "consume multiple events with a single request" in {

    // Create the publisher (read from Nakadi stream)
    val nakadiPublisher = nakadi.consume(createConsumerProperties.copy(offset = Some(Offset("10"))))
    val resultFut = Source
      .fromPublisher(nakadiPublisher)
      .via(Flow[ConsumerMessage].take(10))
      .runWith(Sink.seq)

    // Create the subscriber (write to Nakadi)
    val nakadiSubscriber = nakadi.publish(createProducerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)
    val probe = TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    val events = (1 to 10).map { _ =>
      val event = generateEvent
      event.metadata.eid -> event
    }.toMap
    probe.sendNext(events.values.to[collection.immutable.Seq])

    val result = Await.result(resultFut, 30.seconds)
    result.zip(Stream from 11).foreach {
      case (res, idx) =>
        res.eventType should === (eventType)
        res.cursor should === (Cursor(partition = "0", offset = Offset(idx.toString)))
        validateEvent(res.events.head, events(res.events.head.metadata.eid))
    }
  }

  it should "consume a thousand events" in {

    // Create the publisher (read from Nakadi stream)
    val nakadiPublisher = nakadi.consume(createConsumerProperties.copy(offset = Some(Offset("20"))))
    val resultFut = Source
      .fromPublisher(nakadiPublisher)
      .via(Flow[ConsumerMessage].take(1000))
      .runWith(Sink.seq)

    // Create the subscriber (write to Nakadi)
    val nakadiSubscriber = nakadi.publish(createProducerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)
    val probe = TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    val events = (1 to 1000).map { _ =>
      val event = generateEvent
      event.metadata.eid -> event
    }.toMap
    probe.sendNext(events.values.to[collection.immutable.Seq])

    val result = Await.result(resultFut, 45.seconds)
    result.zip(Stream from 21).foreach {
      case (res, idx) =>
        res.eventType should === (eventType)
        res.cursor should === (Cursor(partition = "0", offset = Offset(idx.toString)))
        validateEvent(res.events.head, events(res.events.head.metadata.eid))
    }
  }

  "Nakadi Parameters" should "consume 20 events over 2 seperate messages given batch_limit is set to 10, and batch_flush_timeout it set to 600" in {

    // Create the publisher (read from Nakadi stream)
    val nakadiPublisher = nakadi.consume(createConsumerProperties.copy(
      batchLimit = 10,
      batchFlushTimeoutInSeconds = 600.seconds,
      offset = Some(Offset("1020"))
    ))
    val resultFut = Source
      .fromPublisher(nakadiPublisher)
      .via(Flow[ConsumerMessage].take(2))
      .runWith(Sink.seq)

    // Create the subscriber (write to Nakadi)
    val nakadiSubscriber = nakadi.publish(createProducerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)
    val probe = TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    val events = (1 to 20).map { _ =>
      val event = generateEvent
      probe.sendNext(Seq(event))
      event.metadata.eid -> event
    }.toMap

    val result = Await.result(resultFut, 30.seconds)
    result.size should === (2)
    result.head.eventType should === (eventType)
    result.head.cursor should === (Cursor(partition = "0", offset = Offset("1030")))
    result.head.events.foreach(ev => validateEvent(ev, events(ev.metadata.eid)))

    result(1).eventType should === (eventType)
    result(1).cursor should === (Cursor(partition = "0", offset = Offset("1040")))
    result(1).events.foreach(ev => validateEvent(ev, events(ev.metadata.eid)))
  }

  it should "consume 10 separate events given stream_limit is set to 10, and then close the connection" in {

    val nakadiPublisher = nakadi.consume(createConsumerProperties.copy(
      streamLimit = 10,
      offset = Some(Offset("1040"))
    ))
    val resultFut = Source
      .fromPublisher(nakadiPublisher)
      .via(Flow[ConsumerMessage].take(10))
      .runWith(Sink.seq)

    // Create the subscriber (write to Nakadi)
    val nakadiSubscriber = nakadi.publish(createProducerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)
    val probe = TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    val events = (1 to 10).map { _ =>
      val event = generateEvent
      probe.sendNext(Seq(event))
      event.metadata.eid -> event
    }.toMap

    val result = Await.result(resultFut, 30.seconds)
    result.size should === (10)
    result.zip(Stream.range(1041, 1050)).foreach {
      case (res, idx) =>
        res.eventType should === (eventType)
        res.cursor should === (Cursor(partition = "0", offset = Offset(idx.toString)))
        validateEvent(res.events.head, events(res.events.head.metadata.eid))
    }
  }

  it should "consume 10 events then close the connection, given stream_keep_alive_limit is set to 5" in {

    val nakadiPublisher = nakadi.consume(createConsumerProperties.copy(
      streamKeepAliveLimit = 5,
      offset = Some(Offset("1050"))
    ))
    val resultFut = Source
      .fromPublisher(nakadiPublisher)
      .via(Flow[ConsumerMessage].take(10))
      .runWith(Sink.seq)

    // Create the subscriber (write to Nakadi)
    val nakadiSubscriber = nakadi.publish(createProducerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)
    val probe = TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    val events = (1 to 10).map { _ =>
      val event = generateEvent
      probe.sendNext(Seq(event))
      event.metadata.eid -> event
    }.toMap

    val result = Await.result(resultFut, 30.seconds)
    result.size should === (10)
    result.zip(Stream.range(1051, 1060)).foreach {
      case (res, idx) =>
        res.eventType should === (eventType)
        res.cursor should === (Cursor(partition = "0", offset = Offset(idx.toString)))
        validateEvent(res.events.head, events(res.events.head.metadata.eid))
    }
  }

  it should "consume 10 events then close the connection after 10 seconds, given stream_timeout is set to 10" in {

    val nakadiPublisher = nakadi.consume(createConsumerProperties.copy(
      streamTimeoutInSeconds = 15.seconds,
      batchFlushTimeoutInSeconds = 10.seconds,
      offset = Some(Offset("1060"))
    ))
    val resultFut = Source
      .fromPublisher(nakadiPublisher)
      .via(Flow[ConsumerMessage].take(10))
      .runWith(Sink.seq)

    // Create the subscriber (write to Nakadi)
    val nakadiSubscriber = nakadi.publish(createProducerProperties)
    val sink = Sink.fromSubscriber(nakadiSubscriber)
    val probe = TestSource.probe[Seq[Event]]
      .map(ProducerMessage(_))
      .toMat(sink)(Keep.left)
      .run()

    val events = (1 to 10).map { _ =>
      val event = generateEvent
      probe.sendNext(Seq(event))
      event.metadata.eid -> event
    }.toMap

    val result = Await.result(resultFut, 30.seconds)
    result.size should === (10)
    result.zip(Stream.range(1061, 1070)).foreach {
      case (res, idx) =>
        res.eventType should === (eventType)
        res.cursor should === (Cursor(partition = "0", offset = Offset(idx.toString)))
        validateEvent(res.events.head, events(res.events.head.metadata.eid))
    }
  }

  it should "publish some events and return committed event ids" in {
    val flow = nakadi.producerFlow[Seq[String]](createProducerProperties)

    val events = (1 to 10).map { _ =>
      val event = generateEvent
      ProducerMessage(Seq(event)) -> Seq(event.metadata.eid)
    }.toList

    val future = Source(events).via(flow).map(_.take(10)).runWith(Sink.seq)

    val committedEventIds = Await.result(future, 20.seconds)
    committedEventIds.flatten.toSet shouldEqual events.flatMap(_._2).toSet
  }
}
