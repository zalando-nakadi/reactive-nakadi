package de.zalando.react.nakadi

import akka.actor.SupervisorStrategy._
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}

import scala.concurrent.duration._


object LeaseManagerActor {

  def props(consumerProperties: ConsumerProperties) = {
    Props(new LeaseManagerActor(consumerProperties))
  }
}

class LeaseManagerActor(consumerProperties: ConsumerProperties) extends Actor with ActorLogging {

  private val leaseManager = LeaseManager(
    consumerProperties.leaseHolder, consumerProperties.commitHandler, consumerProperties.staleLeaseDelta
  )

  private def registerNakadiPublisher(consumerActor: ActorRef, props: Props) = {
    log.info(s"Registering Nakadi Publisher $consumerActor")
    consumerActor ! context.actorOf(props, "nakadi-consumer")
  }

  override def receive: Receive = {
    case p: Props => registerNakadiPublisher(sender, p)
  }

  override val supervisorStrategy = {
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: Exception => Escalate
    }
  }
}
