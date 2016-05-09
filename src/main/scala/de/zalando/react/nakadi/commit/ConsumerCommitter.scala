package de.zalando.react.nakadi.commit

import akka.actor._
import akka.actor.Status.Failure
import de.zalando.react.nakadi.NakadiMessages.ConsumerMessage
import de.zalando.react.nakadi.NakadiActorPublisher.{CommitAck, CommitOffsets}
import de.zalando.react.nakadi.properties.ConsumerProperties

import scala.concurrent.duration._

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

  import ConsumerCommitter.Contract._

  val topic = consumerProperties.topic
  val commitInterval = consumerProperties.commitInterval
  var scheduledFlush: Option[Cancellable] = None
  var partitionOffsetMap = OffsetMap()
  var committedOffsetMap = OffsetMap()
  implicit val executionContext = context.dispatcher

  def scheduleFlush(): Unit = {
    if (scheduledFlush.isEmpty) {
      scheduledFlush = Option(context.system.scheduler.scheduleOnce(commitInterval, self, Flush))
    }
  }

  override def preStart(): Unit = {
    context.watch(consumerActor)
    super.preStart()
  }

  override def postStop(): Unit = {
    scheduledFlush.foreach(_.cancel())
    super.postStop()
  }

  override def receive: Receive = {
    case msg: ConsumerMessage => registerCommit(msg)
    case CommitAck(offsetMap) => handleAcknowledge(offsetMap)
    case Flush                => commitGatheredOffsets()
    case TheEnd =>
      log.debug("Closing Consumer connection")
      context.stop(self)
    case Failure =>
      log.error("Closing offset committer due to a failure")
      context.stop(self)
    case Terminated(_) =>
      log.warning("Terminating the consumer committer due to the death of the consumer actor.")
      context.stop(self)
  }

  def registerCommit(msg: ConsumerMessage): Unit = {
    log.debug(s"Received commit request for partition ${msg.cursor.partition} and offset ${msg.cursor.offset}")
    val topicPartition = TopicPartition(msg.topic, msg.cursor.partition)
    val last = partitionOffsetMap.lastOffset(topicPartition)
    updateOffsetIfLarger(msg, last)
  }

  def updateOffsetIfLarger(msg: ConsumerMessage, last: Long): Unit = {
    val msgOffset = msg.cursor.offset.toLong
    if (msgOffset > last) {
      log.debug(s"Registering commit for partition ${msg.cursor.partition} and offset ${msg.cursor.offset}, last registered = $last")
      val topicPartition = TopicPartition(msg.topic, msg.cursor.partition)
      partitionOffsetMap = partitionOffsetMap.plusOffset(topicPartition, msgOffset)
      scheduleFlush()
    } else {
      log.debug(s"Skipping commit for partition ${msg.cursor.partition} and offset ${msg.cursor.offset}, last registered is $last")
    }
  }

  def handleAcknowledge(offsetMap: OffsetMap) = committedOffsetMap = OffsetMap(offsetMap.map.mapValues(_ - 1))

  def commitGatheredOffsets() = {
    log.debug("Flushing offsets to commit")
    scheduledFlush = None
    val offsetMapToFlush = partitionOffsetMap diff committedOffsetMap
    if (offsetMapToFlush.nonEmpty) {
      consumerActor ! CommitOffsets(offsetMapToFlush)
    }
  }
}
