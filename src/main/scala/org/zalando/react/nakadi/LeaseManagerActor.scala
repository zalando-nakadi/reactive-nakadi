package org.zalando.react.nakadi

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.zalando.react.nakadi.commit.OffsetMap

import scala.util.{Failure, Success}


object LeaseManagerActor {

  case object LeaseAvailable
  case object LeaseUnavailable
  case class RequestLease(groupId: String, eventType: String, partitionId: String)
  case class ReleaseLease(groupId: String, eventType: String, partitionId: String)
  case class Flush(groupId: String, eventType: String, partitionId: String, offsetMap: OffsetMap)

  def props(leaseManager: LeaseManager) = {
    Props(new LeaseManagerActor(leaseManager))
  }
}

class LeaseManagerActor(leaseManager: LeaseManager) extends Actor with ActorLogging {
  import context.dispatcher  // TODO setup different execution contexts for aws access
  import LeaseManagerActor._

  override def receive: Receive = {
    case msg: Flush => flush(msg)
    case msg: RequestLease  => requestLease(msg)
    case msg: ReleaseLease  => releaseLease(msg)
  }

  private def flush(msg: Flush) = {
    val senderRef = sender
    leaseManager.flush(msg.groupId, msg.eventType, msg.partitionId, msg.offsetMap).onComplete {
      case Failure(err) => log.error(err, "Lease Management error when flushing:")
      case Success(status) if status => senderRef ! LeaseAvailable
      case Success(status) if !status => {
        log.error(s"Lease is not usable for event-type '${msg.eventType}' partition '${msg.partitionId}' group '${msg.groupId}'")
        senderRef ! LeaseUnavailable
      }
    }
  }

  private def requestLease(msg: RequestLease): Unit = {
    val senderRef = sender
    leaseManager.requestLease(msg.groupId, msg.eventType, msg.partitionId).onComplete {
      case Failure(err) => log.error(err, "Lease Management error when requesting lease:")
      case Success(status) if status => senderRef ! LeaseAvailable
      case Success(status) if !status => {
        log.error(s"Lease is not usable for event-type '${msg.eventType}' partition '${msg.partitionId}' group '${msg.groupId}'")
        senderRef ! LeaseUnavailable
      }
    }
  }

  private def releaseLease(msg: ReleaseLease): Unit = {
    val senderRef = sender
    leaseManager.releaseLease(msg.groupId, msg.eventType, msg.partitionId).onComplete {
      case Failure(err) => log.error(err, "Lease Management error when releasing lease:")
      case Success(status) => senderRef ! LeaseUnavailable
    }
  }

  private def sendLeaseUnavailable(senderRef: ActorRef) = {
    sys.error("Lease is not usable right now")
    senderRef ! LeaseUnavailable
  }
}
