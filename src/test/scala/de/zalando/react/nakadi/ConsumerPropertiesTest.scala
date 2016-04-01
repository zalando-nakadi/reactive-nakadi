package de.zalando.react.nakadi

import java.util.UUID

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
      securedConnection = true,
      tokenProvider = Option(() => token),
      groupId = grouoId,
      partition = partition,
      commitHandler = InMemoryCommitHandler,
      topic = topic
    )
    props.server should === (server)
    props.port should === (443)
    props.securedConnection should === (true)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.partition should === (partition)
    props.commitHandler should === (InMemoryCommitHandler)
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

  it should "also be able to handle special cases" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = Option(() => token),
      groupId = grouoId,
      partition = partition,
      commitHandler = InMemoryCommitHandler,
      topic = topic
    ).commitInterval(10.seconds)
      .readFromStartOfStream()
      .withPort(9999)
      .withUrlSchema("http://")

    props.server should === (server)
    props.port should === (9999)
    props.securedConnection should === (false)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.partition should === (partition)
    props.commitHandler should === (InMemoryCommitHandler)
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
    props.urlSchema should === ("http://")
  }

  it should "also be able to handle unsecure connection setting" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = Option(() => token),
      groupId = grouoId,
      partition = partition,
      commitHandler = InMemoryCommitHandler,
      topic = topic,
      connectionTimeout = 200.milliseconds
    ).commitInterval(10.seconds)
      .readFromStartOfStream()

    props.server should === (server)
    props.port should === (80)
    props.securedConnection should === (false)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.partition should === (partition)
    props.commitHandler should === (InMemoryCommitHandler)
    props.offset.get.toString should === ("BEGIN")
    props.commitInterval should === (10.seconds)
    props.connectionTimeout should === (200.milliseconds)
    props.batchLimit should === (0)
    props.batchFlushTimeoutInSeconds should === (30.seconds)
    props.streamLimit should === (0)
    props.streamTimeoutInSeconds should === (0.seconds)
    props.streamKeepAliveLimit should === (0)
    props.pollParallelism should === (0)
    props.autoReconnect should === (false)
    props.acceptAnyCertificate should === (true)
  }

  it should "also be able to handle secure connection setting" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = true,
      tokenProvider = Option(() => token),
      groupId = grouoId,
      partition = partition,
      commitHandler = InMemoryCommitHandler,
      topic = topic
    )

    props.server should === (server)
    props.port should === (443)
    props.securedConnection should === (true)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.partition should === (partition)
    props.commitHandler should === (InMemoryCommitHandler)
    props.acceptAnyCertificate should === (true)
    props.urlSchema should === ("https://")
  }

  it should "handle an empty token provider" in {
    val props = ProducerProperties(
      server = server,
      securedConnection = true,
      tokenProvider = None,
      topic = topic
    )
    props.tokenProvider should === (None)
  }

  it should "also should throw exception if invalid url schema passed in" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = None,
      groupId = grouoId,
      partition = partition,
      commitHandler = InMemoryCommitHandler,
      topic = topic
    )
    intercept[IllegalArgumentException](props.withUrlSchema("someblah://"))

  }
}