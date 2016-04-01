package de.zalando.react.nakadi.client

import akka.actor.{ActorRef, ActorSystem}
import de.zalando.react.nakadi.NakadiMessages.ProducerMessage
import de.zalando.react.nakadi.{ConsumerProperties, ProducerProperties}

import scala.concurrent.Future


trait NakadiClient {

  def publishEvent(producerMessage: ProducerMessage): Future[Boolean]

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
