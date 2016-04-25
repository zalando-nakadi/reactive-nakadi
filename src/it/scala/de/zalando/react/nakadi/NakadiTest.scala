package de.zalando.react.nakadi

import akka.testkit.TestKit
import akka.stream.ActorMaterializer
import akka.actor.{ActorSystem, Props}
import akka.stream.actor.WatermarkRequestStrategy

import play.api.libs.json.Json
import com.typesafe.config.{Config, ConfigFactory}

import de.zalando.react.nakadi.NakadiMessages.EventTypeMessage
import de.zalando.react.nakadi.client.NakadiClientImpl
import de.zalando.react.nakadi.commit.handlers.InMemoryCommitHandler

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, OptionValues}

import scala.annotation.tailrec
import scala.concurrent.duration._


trait NakadiTest extends FlatSpec
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures
  with OptionValues {

  val config: Config = ConfigFactory.load()
  val globalTimeout = 2.second

  implicit def system: ActorSystem
  implicit lazy val materializer = ActorMaterializer()
  implicit val executionContext = scala.concurrent.ExecutionContext.global

  def defaultWatermarkStrategy = () => WatermarkRequestStrategy(10)
  def dockerProvider: DockerProvider

  val topic = "nakadi-test-topic"
  val group = "nakadi-test-group"

  val nakadiHost = s"http://${sys.env("DOCKER_IP")}:${config.getString("docker.nakadi.port")}"
  val nakadi = new ReactiveNakadi()

  def createProducerProperties: ProducerProperties = {
    ProducerProperties(
      server = nakadiHost,
      tokenProvider = None,
      topic = topic
    )
  }

  def createConsumerProperties: ConsumerProperties = {
    ConsumerProperties(
      server = nakadiHost,
      tokenProvider = None,
      topic = topic,
      groupId = group,
      partition = "0", // TODO - remove this, should be auto assigned
      commitHandler = InMemoryCommitHandler
    ).commitInterval(2.seconds).readFromStartOfStream()
  }

  def createSubscriberProps(nakadi: ReactiveNakadi, producerProperties: ProducerProperties): Props = {
    nakadi.producerActorProps(producerProperties, requestStrategy = defaultWatermarkStrategy)(system)
  }

  def createConsumerActorProps(nakadi: ReactiveNakadi, consumerProperties: ConsumerProperties): Props = {
    nakadi.consumerActorProps(consumerProperties)(system)
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
    dockerProvider.stop
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  override def beforeAll(): Unit = {
    dockerProvider.start
    createTopic()
  }

  private def createTopic(): Unit = {
    import de.zalando.react.nakadi.client.Properties
    import de.zalando.react.nakadi.client.models.{EventType, EventTypeCategoryEnum}

    val properties = Properties(
      server = nakadiHost,
      tokenProvider = None,
      acceptAnyCertificate = true,
      connectionTimeout = globalTimeout
    )

    val rawSchema =
      """
        |{
        |   "type": "json_schema",
        |   "schema": "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}}, \"required\": [\"foo\"]}"
        |}
      """.stripMargin

    val message = EventTypeMessage(EventType(
      name = topic,
      statistics = None,
      partitionKeyFields = Nil,
      dataKeyFields = None,
      owningApplication = "nakadi-test",
      validationStrategies = None,
      partitionResolutionStrategy = None,
      schema = Option(Json.parse(rawSchema)),
      category = EventTypeCategoryEnum.Data,
      enrichmentStrategies = Seq("metadata_enrichment")
    ))

    system.actorOf(Props(new NakadiClientImpl(properties))) ! message // FIME - implement some sort of acknowledgement

    Thread.sleep(5000) // Hack - sleep a few seconds to wait for event type to be created
  }

}
