package org.zalando.react.nakadi.client

import akka.actor.{ActorRef, ActorSystem}
import org.zalando.react.nakadi.NakadiMessages.{EventTypeMessage, ProducerMessage}
import org.zalando.react.nakadi.properties.{ConsumerProperties, ProducerProperties}

import scala.concurrent.Future


trait NakadiClient {

  def postEventType(eventTypeMessage: EventTypeMessage): Future[Unit]

  def publishEvent(producerMessage: ProducerMessage): Future[Unit]

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
