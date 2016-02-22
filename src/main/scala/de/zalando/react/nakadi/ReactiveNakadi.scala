package de.zalando.react.nakadi

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.actor.{ActorSubscriber, RequestStrategy, WatermarkRequestStrategy, ActorPublisher}
import de.zalando.react.nakadi.NakadiMessages.{ConsumerMessage, ProducerMessage}
import org.reactivestreams.{Publisher, Subscriber}


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

}

object ReactiveNakadi {
  val DefaultRequestStrategy = () => WatermarkRequestStrategy(10)
  val ConsumerDefaultDispatcher = "nakadi-publisher-dispatcher"
  val ProducerDefaultDispatcher = "nakadi-subscriber-dispatcher"
}
