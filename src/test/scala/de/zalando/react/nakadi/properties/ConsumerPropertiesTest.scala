package de.zalando.react.nakadi.properties

import java.util.UUID

import de.zalando.react.nakadi.commit.handlers.BaseCommitManager
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


class ConsumerPropertiesTest extends FlatSpec with Matchers with MockFactory {

  def uuid() = UUID.randomUUID().toString
  def token = "random_token"
  val server = "some.server.zalando.net"
  val eventType = uuid()
  val groupId = uuid()
  val partition = uuid()
  val commitHandler = mock[BaseCommitManager]
  val serverProperties = ServerProperties(host = server, port = 8080, isConnectionSSL = false)

  "ConsumerProperties" should "handle simple case" in {
    val props = ConsumerProperties(
      serverProperties = serverProperties,
      tokenProvider = Option(() => token),
      groupId = groupId,
      partition = partition,
      commitHandler = commitHandler,
      eventType = eventType
    )
    props.serverProperties should === (serverProperties)
    props.tokenProvider.get.apply should === (token)
    props.eventType should === (eventType)
    props.groupId should === (groupId)
    props.partition should === (partition)
    props.commitHandler should === (commitHandler)
    props.offset should === (None)
    props.commitInterval should === (30.seconds)
    props.batchLimit should === (0)
    props.batchFlushTimeoutInSeconds should === (30.seconds)
    props.streamLimit should === (0)
    props.streamTimeoutInSeconds should === (0.seconds)
    props.streamKeepAliveLimit should === (0)
    props.pollParallelism should === (0)
  }

  it should "also be able to set read from end of stream and commitInterval" in {
    val props = ConsumerProperties(
      serverProperties = serverProperties,
      tokenProvider = Option(() => token),
      groupId = groupId,
      partition = partition,
      commitHandler = commitHandler,
      eventType = eventType
    ).commitInterval(10.seconds)
      .readFromStartOfStream()

    props.serverProperties should === (serverProperties)
    props.tokenProvider.get.apply should === (token)
    props.eventType should === (eventType)
    props.groupId should === (groupId)
    props.partition should === (partition)
    props.commitHandler should === (commitHandler)
    props.offset.get.toString should === ("BEGIN")
    props.commitInterval should === (10.seconds)
    props.batchLimit should === (0)
    props.batchFlushTimeoutInSeconds should === (30.seconds)
    props.streamLimit should === (0)
    props.streamTimeoutInSeconds should === (0.seconds)
    props.streamKeepAliveLimit should === (0)
    props.pollParallelism should === (0)
  }

}