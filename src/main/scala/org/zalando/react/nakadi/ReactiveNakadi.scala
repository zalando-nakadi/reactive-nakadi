package org.zalando.react.nakadi

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.util.FastFuture
import akka.stream.{Supervision, ActorAttributes, ActorMaterializer}
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.scaladsl.{Sink, Flow}
import org.reactivestreams.{Publisher, Subscriber}
import org.zalando.react.nakadi.NakadiMessages.{ConsumerMessage, ProducerMessage}
import org.zalando.react.nakadi.client._
import org.zalando.react.nakadi.commit.{CommitSink, NakadiSink}
import org.zalando.react.nakadi.properties.{ConsumerProperties, ProducerProperties}
import play.api.libs.json.Json

import scala.util.{Failure, Success}


class ReactiveNakadi {

  def consume(props: ConsumerProperties)(implicit actorSystem: ActorSystem): Publisher[ConsumerMessage] = {
    ActorPublisher[ConsumerMessage](consumerActor(props))
  }

  def consume(props: ConsumerProperties, dispatcher: String)(implicit actorSystem: ActorSystem): Publisher[ConsumerMessage] = {
    ActorPublisher[ConsumerMessage](consumerActor(props, dispatcher))
  }

  def publish(props: ProducerProperties, requestStrategy: () => RequestStrategy)(implicit actorSystem: ActorSystem): Subscriber[ProducerMessage] = {
    ActorSubscriber[ProducerMessage](producerActor(props, requestStrategy))
  }

  def publish(props: ProducerProperties, requestStrategy: () => RequestStrategy, dispatcher: String)(implicit actorSystem: ActorSystem): Subscriber[ProducerMessage] = {
    ActorSubscriber[ProducerMessage](producerActor(props, requestStrategy, dispatcher))
  }

  def publish(props: ProducerProperties, dispatcher: String)(implicit actorSystem: ActorSystem): Subscriber[ProducerMessage] = {
    ActorSubscriber[ProducerMessage](producerActor(props, dispatcher))
  }

  def publish(props: ProducerProperties)(implicit actorSystem: ActorSystem): Subscriber[ProducerMessage] = {
    ActorSubscriber[ProducerMessage](producerActor(props))
  }

  def consumerActor(props: ConsumerProperties)(implicit actorSystem: ActorSystem): ActorRef = {
    consumerActor(props, ReactiveNakadi.ConsumerDefaultDispatcher)
  }

  def consumerActor(props: ConsumerProperties, dispatcher: String)(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(consumerActorProps(props).withDispatcher(dispatcher))
  }

  def consumerActorProps(props: ConsumerProperties)(implicit actorSystem: ActorSystem) = {
    val consumer = ReactiveNakadiConsumer(props, actorSystem)
    NakadiActorPublisher.props(consumer, LeaseManager(props, actorSystem))
  }

  def producerActor(props: ProducerProperties, requestStrategy: () => RequestStrategy)(implicit actorSystem: ActorSystem): ActorRef = {
    producerActor(props, requestStrategy, ReactiveNakadi.ProducerDefaultDispatcher)
  }

  def producerActor(props: ProducerProperties, dispatcher: String)(implicit actorSystem: ActorSystem): ActorRef = {
    producerActor(props, ReactiveNakadi.DefaultRequestStrategy, dispatcher)
  }

  def producerActor(props: ProducerProperties, requestStrategy: () => RequestStrategy, dispatcher: String)(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(producerActorProps(props, requestStrategy).withDispatcher(dispatcher))
  }

  def producerActor(props: ProducerProperties)(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(producerActorProps(props))
  }

  def producerActorProps(props: ProducerProperties)(implicit actorSystem: ActorSystem) = {
    val producer = new ReactiveNakadiProducer(props, actorSystem)
    NakadiActorSubscriber.props(producer, ReactiveNakadi.DefaultRequestStrategy)
  }

  def producerActorProps(props: ProducerProperties, requestStrategy: () => RequestStrategy)(implicit actorSystem: ActorSystem) = {
    val producer = new ReactiveNakadiProducer(props, actorSystem)
    NakadiActorSubscriber.props(producer, requestStrategy)
  }

  def consumeWithOffsetSink(props: ConsumerProperties, dispatcher: String)(implicit actorSystem: ActorSystem): PublisherWithCommitSink = {
    val conActor: ActorRef = consumerActor(props, dispatcher)
    PublisherWithCommitSink(
      ActorPublisher[ConsumerMessage](conActor),
      conActor,
      CommitSink.create(conActor, props, dispatcher)
    )
  }

  def consumeWithOffsetSink(props: ConsumerProperties)(implicit actorSystem: ActorSystem): PublisherWithCommitSink = {
    consumeWithOffsetSink(props, ReactiveNakadi.ConsumerDefaultDispatcher)
  }

  def producerFlow(producerProperties: ProducerProperties)(implicit system: ActorSystem): Flow[ProducerMessage, Seq[String], NotUsed] = {
    import org.zalando.react.nakadi.client.models.JsonOps._


    import akka.http.scaladsl.model.MediaTypes._
    import akka.http.scaladsl.model.headers._
    import concurrent.duration._

    implicit val materializer = ActorMaterializer()

    val superPool = Http().superPool[Seq[String]]()

    val uri = s"${producerProperties.serverProperties}${URI_POST_EVENTS.format(producerProperties.eventType)}"

    val requestWithoutToken = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(uri),
      headers = List(Accept(MediaRange(`application/json`)))
    )

    implicit val ex = system.dispatcher

    val request = producerProperties.tokenProvider.fold(requestWithoutToken) { tokenFactory =>
      requestWithoutToken.addHeader(Authorization(OAuth2BearerToken(tokenFactory())))
    }

    val flow: Flow[ProducerMessage, Seq[String], NotUsed] = Flow[ProducerMessage]
      .map { case ProducerMessage(eventRecords) =>
        val requestWithEntity = request.withEntity(
          HttpEntity(ContentType(`application/json`),
            Json.toJson(eventRecords).toString)
        )

        eventRecords.headOption.flatMap(_.metadata.flow_id).fold(requestWithEntity) { flowId =>
          requestWithEntity.withHeaders(RawHeader("X-Flow-Id", flowId))
        } -> eventRecords.map(_.metadata.eid)
      }
      .via(superPool)
        .mapAsync(4) {
          case (Success(response), eventIds) if response.status.isSuccess() =>
            response.entity.dataBytes.runWith(Sink.ignore) map (_ => eventIds)

          case (Success(response), eventIds) =>
            response.entity.toStrict(1.second).flatMap { entity =>
              val body = entity.data.utf8String
              val message = s"Request failed for $uri, response code: ${response.status}. Response: $body"
              system.log.warning(message)
              FastFuture.failed(new RuntimeException(message))
            }

          case (Failure(exception), eventIds) =>
              val message = s"Exception during sending request failed for $uri. Message: ${exception.getMessage}"
              system.log.warning(message)
              FastFuture.failed(exception)
        }

    flow.withAttributes(ActorAttributes.supervisionStrategy(Supervision.stoppingDecider))

  }

}

object ReactiveNakadi {
  val DefaultRequestStrategy = () => WatermarkRequestStrategy(10)
  val ConsumerDefaultDispatcher = "nakadi-publisher-dispatcher"
  val ProducerDefaultDispatcher = "nakadi-subscriber-dispatcher"
}

case class PublisherWithCommitSink(
  publisher: Publisher[ConsumerMessage],
  publisherActor: ActorRef,
  nakadiOffsetCommitSink: NakadiSink[ConsumerMessage]) {

  def offsetCommitSink = nakadiOffsetCommitSink.sink

  def cancel(): Unit = {
    publisherActor ! NakadiActorPublisher.Stop
    nakadiOffsetCommitSink.underlyingCommitterActor ! PoisonPill
  }
}

