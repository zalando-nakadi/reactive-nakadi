package de.zalando.react.nakadi

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.actor.{WatermarkRequestStrategy, ActorPublisher}

import de.zalando.react.nakadi.client.models.EventStreamBatch


class ReactiveNakadi {

  def consume(props: ConsumerProperties)(implicit actorSystem: ActorSystem) = {
    ActorPublisher[EventStreamBatch](consumerActor(props))
  }

  def consume(props: ConsumerProperties, dispatcher: String)(implicit actorSystem: ActorSystem) = {
    ActorPublisher[EventStreamBatch](consumerActor(props, dispatcher))
  }

  def consumerActor(props: ConsumerProperties)(implicit actorSystem: ActorSystem): ActorRef = {
    consumerActor(props, ReactiveNakadi.ConsumerDefaultDispatcher)
  }

  def consumerActor(props: ConsumerProperties, dispatcher: String)(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(consumerActorProps(props).withDispatcher(dispatcher))
  }

  def consumerActorProps(props: ConsumerProperties)(implicit actorSystem: ActorSystem) = {
    val reactiveConsumer = ReactiveNakadiConsumer(props, actorSystem)
    NakadiActorPublisher.props(reactiveConsumer)
  }

}

object ReactiveNakadi {
  val DefaultRequestStrategy = () => WatermarkRequestStrategy(10)
  val ConsumerDefaultDispatcher = "nakadi-publisher-dispatcher"
}
