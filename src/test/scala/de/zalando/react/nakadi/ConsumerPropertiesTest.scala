package de.zalando.react.nakadi

import java.util.UUID

import de.zalando.react.nakadi.NakadiMessages.Topic
import de.zalando.react.nakadi.commit.OffsetTracking
import de.zalando.react.nakadi.commit.handlers.BaseHandler
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._


class ConsumerPropertiesTest extends FlatSpec with Matchers {

  def uuid() = UUID.randomUUID().toString
  def token = "random_token"
  val server = "some.server.zalando.net"
  val topic = uuid()
  val grouoId = uuid()

  object DummyCommitHandler extends BaseHandler {
    override def commitSync(groupId: String, topic: Topic, offsets: Seq[OffsetTracking]): Future[Unit] = ???
  }

  "ConsumerProperties" should "handle simple case" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = true,
      tokenProvider = () => token,
      groupId = grouoId,
      commitHandler = DummyCommitHandler,
      topic = topic
    )
    props.server should === (server)
    props.port should === (443)
    props.securedConnection should === (true)
    props.tokenProvider.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.commitHandler should === (DummyCommitHandler)
    props.offset should === (None)
    props.commitInterval should === (None)
    props.consumerTimeoutSec should === (5.seconds)
    props.batchLimit should === (0)
    props.batchFlushTimeoutInSeconds should === (0.seconds)
    props.streamLimit should === (0)
    props.streamTimeoutInSeconds should === (0.seconds)
    props.streamKeepAliveLimit should === (0)
    props.pollParallelism should === (0)
    props.autoReconnect should === (false)
    props.sslVerify should === (true)
  }

  it should "also be able to handle special cases" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = () => token,
      groupId = grouoId,
      commitHandler = DummyCommitHandler,
      topic = topic
    ).commitInterval(10.seconds)
      .consumerTimeoutSec(20.seconds)
      .readFromStartOfStream()
      .withPort(9999)
      .withUrlSchema("http://")

    props.server should === (server)
    props.port should === (9999)
    props.securedConnection should === (false)
    props.tokenProvider.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.commitHandler should === (DummyCommitHandler)
    props.offset.get.toString should === ("BEGIN")
    props.commitInterval should === (Some(10.seconds))
    props.consumerTimeoutSec should === (20.seconds)
    props.batchLimit should === (0)
    props.batchFlushTimeoutInSeconds should === (0.seconds)
    props.streamLimit should === (0)
    props.streamTimeoutInSeconds should === (0.seconds)
    props.streamKeepAliveLimit should === (0)
    props.pollParallelism should === (0)
    props.autoReconnect should === (false)
    props.sslVerify should === (true)
    props.urlSchema should === ("http://")
  }

  it should "also be able to handle unsecure connection setting" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = () => token,
      groupId = grouoId,
      commitHandler = DummyCommitHandler,
      topic = topic
    ).commitInterval(10.seconds)
      .consumerTimeoutSec(20.seconds)
      .readFromStartOfStream()

    props.server should === (server)
    props.port should === (80)
    props.securedConnection should === (false)
    props.tokenProvider.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.commitHandler should === (DummyCommitHandler)
    props.offset.get.toString should === ("BEGIN")
    props.commitInterval should === (Some(10.seconds))
    props.consumerTimeoutSec should === (20.seconds)
    props.batchLimit should === (0)
    props.batchFlushTimeoutInSeconds should === (0.seconds)
    props.streamLimit should === (0)
    props.streamTimeoutInSeconds should === (0.seconds)
    props.streamKeepAliveLimit should === (0)
    props.pollParallelism should === (0)
    props.autoReconnect should === (false)
    props.sslVerify should === (true)
  }

  it should "also be able to handle secure connection setting" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = true,
      tokenProvider = () => token,
      groupId = grouoId,
      commitHandler = DummyCommitHandler,
      topic = topic
    )

    props.server should === (server)
    props.port should === (443)
    props.securedConnection should === (true)
    props.tokenProvider.apply should === (token)
    props.topic should === (topic)
    props.groupId should === (grouoId)
    props.commitHandler should === (DummyCommitHandler)
    props.sslVerify should === (true)
    props.urlSchema should === ("https://")
  }

  it should "also should throw exception if invalid url schema passed in" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = () => token,
      groupId = grouoId,
      commitHandler = DummyCommitHandler,
      topic = topic
    )
    intercept[IllegalArgumentException](props.withUrlSchema("someblah://"))

  }
}