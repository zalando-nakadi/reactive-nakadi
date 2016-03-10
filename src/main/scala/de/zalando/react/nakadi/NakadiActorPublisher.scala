package de.zalando.react.nakadi

import akka.stream.actor.ActorPublisher
import akka.actor.{ActorLogging, ActorRef, Props}
import de.zalando.react.nakadi.NakadiActorPublisher.{CommitOffsets, CommitAck}

import de.zalando.react.nakadi.commit.OffsetMap
import de.zalando.react.nakadi.client.models.EventStreamBatch
import de.zalando.react.nakadi.client.providers.ConsumeCommand
import de.zalando.react.nakadi.NakadiMessages.{Topic, StringConsumerMessage}

import scala.annotation.tailrec


object NakadiActorPublisher {

  case class CommitOffsets(offsetMap: OffsetMap)
  case class CommitAck(offsetMap: OffsetMap)
  case object Stop

  def props(consumerAndProps: ReactiveNakadiConsumer) = {
    Props(new NakadiActorPublisher(consumerAndProps))
  }
}


class NakadiActorPublisher(consumerAndProps: ReactiveNakadiConsumer) extends ActorPublisher[StringConsumerMessage]
  with ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._

  private val topic: Topic = consumerAndProps.properties.topic
  private val groupId: String = consumerAndProps.properties.groupId
  private val client: ActorRef = consumerAndProps.nakadiClient
  private var streamSupervisor: Option[ActorRef] = None  // TODO - There must be a better way...

  private val MaxBufferSize = 100
  private var buf = Vector.empty[StringConsumerMessage]

  override def preStart() = client ! ConsumeCommand.Start

  override def receive: Receive = {

    case ConsumeCommand.Init                      => registerSupervisor(sender())
    case rawEvent: EventStreamBatch if isActive   => readDemandedItems(rawEvent)
    case Request(_)                               => deliverBuf()
    case SubscriptionTimeoutExceeded              => stop()
    case Cancel                                   => stop()
    case CommitOffsets(offsetMap)                 => executeCommit(offsetMap)
  }

  private def registerSupervisor(ref: ActorRef) = {
    ref ! ConsumeCommand.Acknowledge
    streamSupervisor = Option(ref)
  }

  private def readDemandedItems(rawEvent: EventStreamBatch) = {
    if (buf.size == MaxBufferSize) {
      // Do nothing - we dont want to Acknowledge if buffer is full
    } else {
      val message = toMessage(rawEvent)
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

  private def executeCommit(offsetMap: OffsetMap): Unit = {
    val handler = consumerAndProps.properties.commitHandler
    if (handler.isEmpty) log.warning("There is no commit handler defined")
    else {
      handler.get.commitSync(groupId, topic, offsetMap.toCommitRequestInfo)
      sender() ! CommitAck
    }
  }

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
      cursor = NakadiMessages.Cursor(rawEvent.cursor.partition, rawEvent.cursor.offset),
      events = rawEvent.events.getOrElse(Nil),
      topic = topic
    )
  }

  def stop() = context.stop(self)

}
