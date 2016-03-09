package de.zalando.react.nakadi

import org.reactivestreams.{Publisher, Subscriber}

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.stream.actor.{ActorSubscriber, RequestStrategy, WatermarkRequestStrategy, ActorPublisher}

import de.zalando.react.nakadi.commit.{NakadiSink, CommitSink}
import de.zalando.react.nakadi.NakadiMessages.{ConsumerMessage, ProducerMessage}


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
    NakadiActorPublisher.props(consumer)
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

}

object ReactiveNakadi {
  val DefaultRequestStrategy = () => WatermarkRequestStrategy(10)
  val ConsumerDefaultDispatcher = "nakadi-publisher-dispatcher"
  val ProducerDefaultDispatcher = "nakadi-subscriber-dispatcher"
}

case class PublisherWithCommitSink(
  publisher: Publisher[ConsumerMessage],
  publisherActor: ActorRef,
  kafkaOffsetCommitSink: NakadiSink[ConsumerMessage]) {

  def offsetCommitSink = kafkaOffsetCommitSink.sink

  def cancel(): Unit = {
    publisherActor ! NakadiActorPublisher.Stop
    kafkaOffsetCommitSink.underlyingCommitterActor ! PoisonPill
  }
}

