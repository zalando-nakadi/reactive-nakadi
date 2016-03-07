package de.zalando.react.nakadi

import akka.actor.Actor.Receive
import akka.actor.{Props, ActorRef, ActorLogging, Actor}


object ConsumerCommitter {

  object Contract {
    object TheEnd
    object Flush
  }

  def props(consumerActor: ActorRef, consumerProperties: ConsumerProperties) = {
    Props(new ConsumerCommitter(consumerActor, consumerProperties))
  }
}

class ConsumerCommitter(consumerActor: ActorRef, consumerProperties: ConsumerProperties) extends Actor
  with ActorLogging {

  override def receive: Receive = ???
}
