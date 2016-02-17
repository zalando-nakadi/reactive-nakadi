package de.zalando.react.nakadi

import akka.actor.{ActorRef, ActorSystem}

import scala.concurrent.Future


trait NakadiClient {

  /**
    * Post a single event to the given topic.  Partition selection is done using the defined partition resolution.
    * The partition resolution strategy is defined per topic and is managed by event store (currently resolved from
    * hash over Event.orderingKey).
    *
    * @param name this represents a topic
    * @param event event to be posted
    * @param flowId The flow id of the request, which is written into the logs and passed to called services.
    *               Helpful for operational troubleshooting and log analysis.
    * @return Option representing the error message or None in case of success
    */
  def postEvent(name: String, event: String, flowId: Option[String] = None): Future[Boolean]

  /**
    * Blocking subscription to events of specified topic and partition.
    * (batchLimit is set to 1, batch flush timeout to 1,  and streamLimit to 0 -> infinite streaming receiving 1 event per poll)
    *
    * @return Either error message or connection was closed and reconnect is set to false
    */
  def listenForEvents(): Unit
}


object NakadiClient {

  def apply(consumerProperties: ConsumerProperties, actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(NakadiClientImpl.props(consumerProperties), "nakadi-client")
  }
}
