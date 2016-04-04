package de.zalando.react.nakadi

import java.util.UUID

import de.zalando.react.nakadi.commit.handlers
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


class ConsumerPropertiesTest extends FlatSpec with Matchers {

  def uuid() = UUID.randomUUID().toString
  def token = "random_token"
  val server = "some.server.zalando.net"
  val topic = uuid()
  val grouoId = uuid()
  val partition = uuid()

  "ConsumerProperties" should "handle simple case" in {
    val props = ConsumerProperties(
      server = server,
      tokenProvider = Option(() => token),
      groupId = grouoId,
      partition = partition,
      commitHandler = handlers.InMemoryCommitHandler,
      topic = topic
    )
    props.server should === (server)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.partition should === (partition)
    props.commitHandler should === (handlers.InMemoryCommitHandler)
    props.offset should === (None)
    props.commitInterval should === (30.seconds)
    props.connectionTimeout should === (5000.milliseconds)
    props.batchLimit should === (0)
    props.batchFlushTimeoutInSeconds should === (30.seconds)
    props.streamLimit should === (0)
    props.streamTimeoutInSeconds should === (0.seconds)
    props.streamKeepAliveLimit should === (0)
    props.pollParallelism should === (0)
    props.autoReconnect should === (false)
    props.acceptAnyCertificate should === (true)
  }

  it should "also be able to set read from end of stream and commitInterval" in {
    val props = ConsumerProperties(
      server = server,
      tokenProvider = Option(() => token),
      groupId = grouoId,
      partition = partition,
      commitHandler = handlers.InMemoryCommitHandler,
      topic = topic
    ).commitInterval(10.seconds)
      .readFromStartOfStream()

    props.server should === (server)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.partition should === (partition)
    props.commitHandler should === (handlers.InMemoryCommitHandler)
    props.offset.get.toString should === ("BEGIN")
    props.commitInterval should === (10.seconds)
    props.connectionTimeout should === (5000.milliseconds)
    props.batchLimit should === (0)
    props.batchFlushTimeoutInSeconds should === (30.seconds)
    props.streamLimit should === (0)
    props.streamTimeoutInSeconds should === (0.seconds)
    props.streamKeepAliveLimit should === (0)
    props.pollParallelism should === (0)
    props.autoReconnect should === (false)
    props.acceptAnyCertificate should === (true)
  }

}