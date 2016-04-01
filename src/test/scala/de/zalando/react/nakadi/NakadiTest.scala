package de.zalando.react.nakadi

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.WatermarkRequestStrategy
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.annotation.tailrec
import scala.concurrent.duration._


trait NakadiTest extends BeforeAndAfterAll { this: Suite =>

  implicit def system: ActorSystem
  implicit lazy val materializer = ActorMaterializer()

  case class FixtureParam(topic: String, group: String, nakadi: ReactiveNakadi)

  def defaultWatermarkStrategy = () => WatermarkRequestStrategy(10)

  val nakadiHost = "localhost"
  val nakadiPort = 8080

  val nakadi = new ReactiveNakadi()

  def createProducerProperties(f: FixtureParam): ProducerProperties = {
    ProducerProperties(
      server = nakadiHost,
      port = nakadiPort,
      securedConnection = false,
      tokenProvider = () => "some_token",
      topic = f.topic
    )
  }

  def createConsumerProperties(f: FixtureParam): ConsumerProperties = {
    ConsumerProperties(
      server = nakadiHost,
      port = nakadiPort,
      securedConnection = false,
      tokenProvider = () => "some_token",
      topic = f.topic,
      groupId = f.group,
      partition = "0", // TODO - remove this, should be auto assigned
      commitHandler = InMemoryCommitHandler
    ).commitInterval(2.seconds)
  }

  def createSubscriberProps(nakadi: ReactiveNakadi, producerProperties: ProducerProperties): Props = {
    nakadi.producerActorProps(producerProperties, requestStrategy = defaultWatermarkStrategy)(system)
  }

  def createConsumerActorProps(nakadi: ReactiveNakadi, consumerProperties: ConsumerProperties): Props = {
    nakadi.consumerActorProps(consumerProperties)(system)
  }

  def createTestSubscriber(): ActorRef = {
    system.actorOf(Props(new ReactiveTestSubscriber))
  }

  def stringSubscriber(f: FixtureParam) = {
    f.nakadi.publish(createProducerProperties(f))(system)
  }

  def stringSubscriberActor(f: FixtureParam) = {
    f.nakadi.producerActor(createProducerProperties(f))(system)
  }

  def stringConsumer(f: FixtureParam) = {
    f.nakadi.consume(createConsumerProperties(f))(system)
  }

  def stringConsumerWithOffsetSink(f: FixtureParam) = {
    f.nakadi.consumeWithOffsetSink(createConsumerProperties(f))(system)
  }

  def newNakadi(): ReactiveNakadi = new ReactiveNakadi()

  @tailrec
  final def verifyNever(unexpectedCondition: => Boolean, start: Long = System.currentTimeMillis()): Unit = {
    val now = System.currentTimeMillis()
    if (start + 3000 >= now) {
      Thread.sleep(100)
      if (unexpectedCondition)
        fail("Assertion failed before timeout passed")
      else
        verifyNever(unexpectedCondition, start)
    }
  }

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

}
