package de.zalando.react.nakadi

import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._

import scala.concurrent.duration._


class LeaseManagerActor extends Actor {

  override def receive: Receive = ???

  override val supervisorStrategy = {
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: Exception => Escalate
    }
  }
}
