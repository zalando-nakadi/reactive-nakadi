package de.zalando.react.nakadi.commit

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl.Sink
import de.zalando.react.nakadi.NakadiMessages.ConsumerMessage
import de.zalando.react.nakadi.commit.ConsumerCommitter.Contract.TheEnd
import de.zalando.react.nakadi.properties.ConsumerProperties


private[nakadi] object CommitSink {

  def create(consumerActor: ActorRef,
             consumerProperties: ConsumerProperties,
             customDispatcherName: String)(implicit actorSystem: ActorSystem) = {
    val actor = actorSystem.actorOf(ConsumerCommitter.props(consumerActor, consumerProperties))
    NakadiSink(Sink.actorRef[ConsumerMessage](actor, TheEnd), actor)
  }
}

case class NakadiSink[T](sink: Sink[T, NotUsed], underlyingCommitterActor: ActorRef)
