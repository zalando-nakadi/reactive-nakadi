package de.zalando.react.nakadi

import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.actor.{ActorSystem, ActorRef}

import de.zalando.react.nakadi.NakadiMessages.ConsumerMessage
import de.zalando.react.nakadi.ConsumerCommitter.Contract.TheEnd


private[nakadi] object CommitSink {

  def create(consumerActor: ActorRef,
             consumerProperties: ConsumerProperties,
             customDispatcherName: String)(implicit actorSystem: ActorSystem) = {
    val actor = actorSystem.actorOf(ConsumerCommitter.props(consumerActor, consumerProperties))
    NakadiSink(Sink.actorRef[ConsumerMessage](actor, TheEnd), actor)
  }
}

case class NakadiSink[T](sink: Sink[T, NotUsed], underlyingCommitterActor: ActorRef)
