package de.zalando.react.nakadi

import java.util.UUID

import scala.concurrent.duration._

import org.scalatest.{Matchers, FlatSpec}

class ProducerPropertiesTest extends FlatSpec with Matchers {

  def uuid() = UUID.randomUUID().toString
  def token = "random_token"
  val server = "http://some.server.zalando.net:9089/"
  val topic = uuid()

  "ProducerProperties" should "handle simple case" in {
    val props = ProducerProperties(
      server = server,
      tokenProvider = Option(() => token),
      topic = topic
    )
    props.server should === (server)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.retries should === (None)
    props.acceptAnyCertificate should === (true)
    props.connectionTimeout should === (1000.milliseconds)
  }

  it should "handle an empty token provider" in {
    val props = ProducerProperties(
      server = server,
      tokenProvider = None,
      topic = topic
    )
    props.tokenProvider should === (None)
  }

  it should "also be able to set message send max retries" in {
    val props = ProducerProperties(
      server = server,
      tokenProvider = Option(() => token),
      topic = topic
    ).messageSendMaxRetries(5)

    props.server should === (server)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
    props.retries should === (Some(5))
    props.acceptAnyCertificate should === (true)
    props.connectionTimeout should === (1000.milliseconds)
  }
}
