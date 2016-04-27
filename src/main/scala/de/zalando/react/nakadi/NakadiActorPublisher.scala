package de.zalando.react.nakadi

import akka.stream.actor.ActorPublisher
import akka.actor.{ActorLogging, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import de.zalando.react.nakadi.commit.OffsetMap
import de.zalando.react.nakadi.client.models.{Event, EventStreamBatch}
import de.zalando.react.nakadi.client.providers.ConsumeCommand
import de.zalando.react.nakadi.NakadiMessages.{Offset, StringConsumerMessage}
import de.zalando.react.nakadi.NakadiActorPublisher.{CommitAck, CommitOffsets}

import scala.annotation.tailrec
import scala.util.{Failure, Success}


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

  private val topic: String = consumerAndProps.properties.topic
  private val groupId: String = consumerAndProps.properties.groupId
  private val client: ActorRef = consumerAndProps.nakadiClient
  private var streamSupervisor: Option[ActorRef] = None  // TODO - There must be a better way...

  private val MaxBufferSize = 100
  private var buf = Vector.empty[StringConsumerMessage]

  override def preStart() = {
    // TODO - check lease manager for other consumers
    client ! ConsumeCommand.Start
  }

  override def receive: Receive = {

    case ConsumeCommand.Init                              => registerSupervisor(sender())
    case Some(rawEvent: EventStreamBatch) if isActive     => readDemandedItems(rawEvent)
    case Request(_)                                       => deliverBuf()
    case SubscriptionTimeoutExceeded                      => stop()
    case Cancel                                           => stop()
    case CommitOffsets(offsetMap)                         => executeCommit(offsetMap)
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

  private def executeCommit(offsetMap: OffsetMap): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val senderRef = sender()

    // FIXME - perhaps make the commit handler a separate Actor
    LeaseManager(
      "some-lease-holder",
      consumerAndProps.properties.partition,
      consumerAndProps.properties.commitHandler,
      ConfigFactory.load()
    ).commit(groupId, topic, offsetMap)
      .onComplete {
        case Failure(err) => log.error(err, "AWS Error:")
        case Success(_) => senderRef ! CommitAck
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
      cursor = NakadiMessages.Cursor(rawEvent.cursor.partition, Offset(rawEvent.cursor.offset)),
      events = rawEvent.events.getOrElse(Nil),
      topic = topic
    )
  }

  def stop() = context.stop(self)

}
