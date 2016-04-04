package de.zalando.react.nakadi

import java.util.UUID

import scala.concurrent.duration._

import org.scalatest.{Matchers, FlatSpec}

class ProducerPropertiesTest extends FlatSpec with Matchers {

  def uuid() = UUID.randomUUID().toString
  def token = "random_token"
  val server = "some.server.zalando.net"
  val topic = uuid()

  "ProducerProperties" should "handle simple case" in {
    val props = ProducerProperties(
      server = server,
      securedConnection = true,
      tokenProvider = Option(() => token),
      topic = topic
    )
    props.server should === (server)
    props.port should === (443)
    props.securedConnection should === (true)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.retries should === (None)
    props.acceptAnyCertificate should === (true)
    props.urlSchema should === ("https://")
    props.connectionTimeout should === (1000.milliseconds)
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

  it should "also be able to handle special cases" in {
    val props = ProducerProperties(
      server = server,
      securedConnection = true,
      tokenProvider = Option(() => token),
      topic = topic
    ).withPort(9999)
      .messageSendMaxRetries(5)

    props.server should === (server)
    props.port should === (9999)
    props.securedConnection should === (true)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.retries should === (Some(5))
    props.acceptAnyCertificate should === (true)
    props.urlSchema should === ("https://")
    props.connectionTimeout should === (1000.milliseconds)
  }

  it should "also be able to handle unsecure connection setting" in {
    val props = ProducerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = Option(() => token),
      topic = topic
    )
    props.server should === (server)
    props.port should === (80)
    props.securedConnection should === (false)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.retries should === (None)
    props.acceptAnyCertificate should === (true)
    props.urlSchema should === ("http://")
    props.connectionTimeout should === (1000.milliseconds)
  }

  it should "also should throw exception if invalid url schema passed in" in {
    val props = ProducerProperties(
      server = server,
      securedConnection = false,
      tokenProvider = Option(() => token),
      topic = topic
    )
    intercept[IllegalArgumentException](props.withUrlSchema("someblah://"))
  }
}
