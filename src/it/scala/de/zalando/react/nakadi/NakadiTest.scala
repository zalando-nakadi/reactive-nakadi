package de.zalando.react.nakadi

import akka.testkit.TestKit
import akka.stream.ActorMaterializer
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.stream.actor.WatermarkRequestStrategy
import play.api.libs.json.Json
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, OptionValues}
import de.zalando.react.nakadi.client.NakadiClientImpl
import de.zalando.react.nakadi.NakadiMessages.EventTypeMessage
import de.zalando.react.nakadi.properties.{ConsumerProperties, ProducerProperties, ServerProperties}

import scala.concurrent.duration._
import scala.de.zalando.react.nakadi.InMemoryCommitLeaseManager


trait NakadiTest extends FlatSpec
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures
  with OptionValues
  with MockFactory {

  val config: Config = ConfigFactory.load()

  implicit def system: ActorSystem
  implicit lazy val materializer = ActorMaterializer()
  implicit val executionContext = scala.concurrent.ExecutionContext.global

  def defaultWatermarkStrategy = () => WatermarkRequestStrategy(10)
  def dockerProvider: DockerProvider

  val eventType = "nakadi-test-event-type"
  val group = "nakadi-test-group"

  val nakadi = new ReactiveNakadi()
  val serverProperties = ServerProperties(
    host = sys.env("DOCKER_IP"),
    port = config.getInt("docker.nakadi.port"),
    isConnectionSSL = false
  )

  lazy val nakadiClientActor: ActorRef = {
    import de.zalando.react.nakadi.client.Properties

    val properties = Properties(
      serverProperties = serverProperties,
      tokenProvider = None
    )

    system.actorOf(Props(new NakadiClientImpl(properties)))
  }

  def createProducerProperties: ProducerProperties = {
    ProducerProperties(
      serverProperties = serverProperties,
      tokenProvider = None,
      eventType = eventType
    )
  }

  def createConsumerProperties: ConsumerProperties = {
    ConsumerProperties(
      serverProperties = serverProperties,
      tokenProvider = None,
      eventType = eventType,
      groupId = group,
      partition = "0", // TODO - remove this, should be auto assigned
      commitHandler = InMemoryCommitLeaseManager
    ).commitInterval(2.seconds).readFromStartOfStream()
  }

  def createSubscriberProps(nakadi: ReactiveNakadi, producerProperties: ProducerProperties): Props = {
    nakadi.producerActorProps(producerProperties, requestStrategy = defaultWatermarkStrategy)(system)
  }

  def createConsumerActorProps(nakadi: ReactiveNakadi, consumerProperties: ConsumerProperties): Props = {
    nakadi.consumerActorProps(consumerProperties)(system)
  }

  def newNakadi(): ReactiveNakadi = new ReactiveNakadi()

  override def afterAll(): Unit = {
    nakadiClientActor ! PoisonPill
    dockerProvider.stop
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  override def beforeAll(): Unit = {
    dockerProvider.start
    createTopic()
  }

  private def createTopic(): Unit = {
    import de.zalando.react.nakadi.client.models.{EventType, EventTypeCategoryEnum}

    val rawSchema =
      """
        |{
        |   "type": "json_schema",
        |   "schema": "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}}, \"required\": [\"foo\"]}"
        |}
      """.stripMargin

    val message = EventTypeMessage(EventType(
      name = eventType,
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

    // FIXME - implement some sort of acknowledgement
    nakadiClientActor ! message

    // Hack - sleep a few seconds to wait for event type to be created
    Thread.sleep(5000)
  }

}
