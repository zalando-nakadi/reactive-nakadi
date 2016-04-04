package de.zalando.react.nakadi

import java.util.UUID

import akka.actor.ActorSystem
import de.zalando.react.nakadi.NakadiMessages.{ConsumerMessage, ProducerMessage}
import org.joda.time.DateTime
import org.reactivestreams.Publisher
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration


class ReactiveNakadiPublisherSpec(defaultTimeout: FiniteDuration)
  extends PublisherVerification[ConsumerMessage](new TestEnvironment(defaultTimeout.toMillis), defaultTimeout.toMillis)
  with TestNGSuiteLike with ReactiveStreamsTckVerificationBase {

  override implicit val system: ActorSystem = ActorSystem("ReactiveNakadiActorPublisherSpec")

  def this() = this(30000.milliseconds)

  def uuid() = UUID.randomUUID().toString
  val flowId = uuid()

  /**
    * This indicates that our publisher cannot provide an onComplete() signal
    */
  override def maxElementsFromPublisher(): Long = Long.MaxValue

  override def createPublisher(elements: Long): Publisher[ConsumerMessage] = {
    import de.zalando.react.nakadi.client.models.{DataOpEnum, Event, MetaData, EventPayload}

    val topic = uuid()
    val group = "group1"

    // Filling the queue with Int.MaxValue elements takes much too long
    // Test case which verifies point 3.17 may as well fill with small amount of elements.
    // It verifies demand overflow which has nothing to do with supply size.
    val realSize = if (elements == Int.MaxValue) 30 else elements
    val properties = createProducerProperties(FixtureParam(topic, group, nakadi))
    val lowLevelProducer = new ReactiveNakadiProducer(properties, system)

    val recorcd = ProducerMessage(Seq(Event(
        data_type = "test_data_type",
        data_op = DataOpEnum.C,
        data = Json.parse(s"""{"foo": "bar"}""").as[EventPayload],
        MetaData(
          eid = UUID.randomUUID().toString,
          occurred_at = new DateTime(),
          flow_id = Option("my-test-flow-id")
        )))
    )
    ???
  }

  override def createFailedPublisher(): Publisher[ConsumerMessage] = ???
}
