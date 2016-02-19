package de.zalando.react.nakadi

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


class ConsumerPropertiesTest extends FlatSpec with Matchers {

  def uuid() = UUID.randomUUID().toString
  def token = "random_token"
  val server = "some.server.zalando.net"
  val topic = uuid()

  "ConsumerProperties" should "handle simple case" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = true,
      tokenProvider = () => token,
      topic = topic
    )
    props.server should === (server)
    props.port should === (443)
    props.securedConnection should === (true)
    props.tokenProvider.apply should === (token)
    props.topic should === (topic)
    props.offset should === ("earliest")
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
      topic = topic
    ).commitInterval(10.seconds)
      .consumerTimeoutSec(20.seconds)
      .readFromEndOfStream
      .withPort(9999)

    props.server should === (server)
    props.port should === (9999)
    props.securedConnection should === (false)
    props.tokenProvider.apply should === (token)
    props.topic should === (topic)
    props.offset should === ("latest")
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

  it should "also be able to handle unsecure connection setting" in {
    val props = ConsumerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = () => token,
      topic = topic
    ).commitInterval(10.seconds)
      .consumerTimeoutSec(20.seconds)
      .readFromEndOfStream

    props.server should === (server)
    props.port should === (80)
    props.securedConnection should === (false)
    props.tokenProvider.apply should === (token)
    props.topic should === (topic)
    props.offset should === ("latest")
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
}