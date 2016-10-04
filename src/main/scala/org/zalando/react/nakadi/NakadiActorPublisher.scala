package org.zalando.react.nakadi

import akka.stream.actor.ActorPublisher
import akka.actor.{ActorLogging, ActorRef, PoisonPill, Props, ReceiveTimeout}
import org.zalando.react.nakadi.commit.OffsetMap
import org.zalando.react.nakadi.LeaseManagerActor.Flush
import org.zalando.react.nakadi.client.models.EventStreamBatch
import org.zalando.react.nakadi.client.providers.ConsumeCommand
import org.zalando.react.nakadi.NakadiActorPublisher.CommitOffsets
import org.zalando.react.nakadi.NakadiMessages.{Offset, StringConsumerMessage}

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal


object NakadiActorPublisher {

  case class CommitOffsets(offsetMap: OffsetMap)
  case class CommitAck(offsetMap: OffsetMap)
  case object Stop

  def props(consumerAndProps: ReactiveNakadiConsumer, leaseManager: ActorRef) = {
    Props(new NakadiActorPublisher(consumerAndProps, leaseManager))
  }
}

case class NakadiTimeout(error: String) extends Exception(error)

class NakadiActorPublisher(consumerAndProps: ReactiveNakadiConsumer, leaseManager: ActorRef) extends ActorPublisher[StringConsumerMessage]
  with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._

  private var isRunning: Boolean = false

  private val eventType: String = consumerAndProps.properties.eventType
  private val groupId: String = consumerAndProps.properties.groupId
  private val partition: String = consumerAndProps.properties.partition
  private val client: ActorRef = consumerAndProps.nakadiClient
  private var streamSupervisor: Option[ActorRef] = None
  private val reconnectTimeout: Duration = consumerAndProps.properties.batchFlushTimeoutInSeconds * 1.5
  private val MaxBufferSize = 100
  private var buf = Vector.empty[StringConsumerMessage]

  override def preStart() = start()

  override def receive: Receive = {

    case ConsumeCommand.Init                              => registerSupervisor(sender())
    case Some(rawEvent: EventStreamBatch) if isActive     => readDemandedItems(rawEvent)
    case Request(_)                                       => deliverBuf()
    case SubscriptionTimeoutExceeded                      => stop()
    case Cancel                                           => stop()
    case NakadiActorPublisher.Stop                        => stop()
    case CommitOffsets(offsetMap)                         => executeCommit(offsetMap)
    case NonFatal(err)                                    => onError(err)
    case ReceiveTimeout                                   => throw NakadiTimeout(s"No events from Nakadi in the last $reconnectTimeout seconds...")
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

  private def executeCommit(offsetMap: OffsetMap) = leaseManager ! Flush(groupId, eventType, partition, offsetMap)

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
      eventType = eventType
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
      context.setReceiveTimeout(reconnectTimeout)
      isRunning = true
      client ! ConsumeCommand.Start
    }
  }
}
