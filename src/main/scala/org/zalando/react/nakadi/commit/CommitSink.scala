package org.zalando.react.nakadi.commit

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl.Sink

import org.zalando.react.nakadi.properties.ConsumerProperties
import org.zalando.react.nakadi.NakadiMessages.ConsumerMessage
import org.zalando.react.nakadi.commit.ConsumerCommitter.Contract.TheEnd


private[nakadi] object CommitSink {

  def create(consumerActor: ActorRef,
             consumerProperties: ConsumerProperties,
             customDispatcherName: String)(implicit actorSystem: ActorSystem) = {
    val actor = actorSystem.actorOf(ConsumerCommitter.props(consumerActor, consumerProperties))
    NakadiSink(Sink.actorRef[ConsumerMessage](actor, TheEnd), actor)
  }
}

case class NakadiSink[T](sink: Sink[T, NotUsed], underlyingCommitterActor: ActorRef)
