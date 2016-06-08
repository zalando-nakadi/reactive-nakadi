package org.zalando.react.nakadi

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import org.reactivestreams.{Publisher, Subscriber}
import org.zalando.react.nakadi.NakadiMessages.{ConsumerMessage, ProducerMessage}
import org.zalando.react.nakadi.client._
import org.zalando.react.nakadi.commit.{CommitSink, NakadiSink}
import org.zalando.react.nakadi.properties.{ConsumerProperties, ProducerProperties}
import akka.pattern.ask
import scala.concurrent.duration._


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

  def producerFlow[T](producerProperties: ProducerProperties)(implicit system: ActorSystem): Flow[(ProducerMessage, T), T, NotUsed] = {
    val producer = new ReactiveNakadiProducer(producerProperties, system)
    implicit val ex = system.dispatcher
    val paralellism = system.settings.config.getInt("producer-flow.nakadi-client-parallelism")

    implicit val timeout = Timeout(
      system.settings.config.getDuration("producer-flow.nakadi-client-parallelism", TimeUnit.MILLISECONDS).milliseconds
    )

    Flow[(ProducerMessage, T)]
      .mapAsync(4) { case (producerMessage, correlationMarker) =>
        (producer.nakadiClient ? producerMessage).map { case NakadiClientImpl.MessagePublished => correlationMarker }
      }
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

