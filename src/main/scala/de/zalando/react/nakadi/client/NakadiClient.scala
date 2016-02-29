package de.zalando.react.nakadi.client

import akka.actor.{ActorRef, ActorSystem}
import de.zalando.react.nakadi.{ProducerProperties, ConsumerProperties}


trait NakadiClient {

  def publishEvent(events: Seq[String], flowId: Option[String] = None): Unit

  def listenForEvents(receiverActorRef: ActorRef): Unit
}


object NakadiClient {

  def apply(consumerProperties: ConsumerProperties, actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(NakadiClientImpl.props(consumerProperties))
  }

  def apply(producerProperties: ProducerProperties, actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(NakadiClientImpl.props(producerProperties))
  }
}
