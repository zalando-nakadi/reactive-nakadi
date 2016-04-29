package de.zalando.react.nakadi

import akka.stream.actor.ActorPublisher
import akka.actor.{ActorLogging, ActorRef, PoisonPill, Props}
import de.zalando.react.nakadi.commit.OffsetMap
import de.zalando.react.nakadi.client.models.EventStreamBatch
import de.zalando.react.nakadi.client.providers.ConsumeCommand
import de.zalando.react.nakadi.NakadiActorPublisher.CommitOffsets
import de.zalando.react.nakadi.NakadiMessages.{Offset, StringConsumerMessage}

import scala.annotation.tailrec


object NakadiActorPublisher {

  case class CommitOffsets(offsetMap: OffsetMap)
  case class CommitAck(offsetMap: OffsetMap)
  case object Stop

  def props(consumerAndProps: ReactiveNakadiConsumer, leaseManager: ActorRef) = {
    Props(new NakadiActorPublisher(consumerAndProps, leaseManager))
  }
}


class NakadiActorPublisher(consumerAndProps: ReactiveNakadiConsumer, leaseManager: ActorRef) extends ActorPublisher[StringConsumerMessage]
  with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._
  import de.zalando.react.nakadi.LeaseManagerActor._

  private var isRunning: Boolean = false

  private val topic: String = consumerAndProps.properties.topic
  private val groupId: String = consumerAndProps.properties.groupId
  private val partition: String = consumerAndProps.properties.partition
  private val client: ActorRef = consumerAndProps.nakadiClient
  private var streamSupervisor: Option[ActorRef] = None

  private val MaxBufferSize = 100
  private var buf = Vector.empty[StringConsumerMessage]

  override def preStart() = leaseManager ! RequestLease(groupId, topic, partition)

  def releaseLease() = leaseManager ! ReleaseLease(groupId, topic, partition)

  override def receive: Receive = {

    case ConsumeCommand.Init                              => registerSupervisor(sender())
    case Some(rawEvent: EventStreamBatch) if isActive     => readDemandedItems(rawEvent)
    case Request(_)                                       => deliverBuf()
    case SubscriptionTimeoutExceeded                      => releaseLease()
    case Cancel                                           => releaseLease()
    case NakadiActorPublisher.Stop                        => releaseLease()
    case CommitOffsets(offsetMap)                         => executeCommit(offsetMap)
    case LeaseAvailable                                   => start()
    case LeaseUnavailable                                 => stop()
  }

  private def registerSupervisor(ref: ActorRef) = {
    ref ! ConsumeCommand.Acknowledge
    streamSupervisor = Option(ref)
  }

  private def readDemandedItems(rawEvent: EventStreamBatch) = {
    val message = toMessage(rawEvent)

    if (buf.size == MaxBufferSize - 1) {
      // We don't want to acknowledge as we're at capacity
      // We fill the last element of the buffer to be processed later
      buf :+= message
    } else {
      sender() ! ConsumeCommand.Acknowledge

      if (message.events.nonEmpty) {
        if (buf.isEmpty && totalDemand > 0) {
          onNext(message)
        }
        else {
          buf :+= message
          deliverBuf()
        }
      }
    }
  }

  private def executeCommit(offsetMap: OffsetMap) = leaseManager ! Flush(groupId, topic, partition, offsetMap)

  @tailrec
  final def deliverBuf(): Unit = {
    if (totalDemand > 0) {
      if (buf.isEmpty) streamSupervisor.foreach(_ ! ConsumeCommand.Acknowledge)
      /*
       * totalDemand is a Long and could be larger than
       * what buf.splitAt can accept
       */
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use.foreach(onNext)
      } else {
        val (use, keep) = buf.splitAt(Int.MaxValue)
        buf = keep
        use.foreach(onNext)
        deliverBuf()
      }

    }
  }

  private def toMessage(rawEvent: EventStreamBatch) = {
    NakadiMessages.ConsumerMessage(
      cursor = NakadiMessages.Cursor(rawEvent.cursor.partition, Offset(rawEvent.cursor.offset)),
      events = rawEvent.events.getOrElse(Nil),
      topic = topic
    )
  }

  def stop() = {
    if (isRunning) {
      client ! PoisonPill
      context.stop(self)
      isRunning = false
    }
  }

  def start() = {
    if (!isRunning) {
      isRunning = true
      client ! ConsumeCommand.Start
    }
  }
}
