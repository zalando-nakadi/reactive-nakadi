package de.zalando.react.nakadi.properties

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}

class ProducerPropertiesTest extends FlatSpec with Matchers {

  def uuid() = UUID.randomUUID().toString
  def token = "random_token"
  val server = "http://some.server.zalando.net:9089/"
  val topic = uuid()
  val serverProperties = ServerProperties(host = server, port = 8080, isConnectionSSL = false)

  "ProducerProperties" should "handle simple case" in {
    val props = ProducerProperties(
      serverProperties = serverProperties,
      tokenProvider = Option(() => token),
      topic = topic
    )
    props.serverProperties should === (serverProperties)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
  }

  it should "handle an empty token provider" in {
    val props = ProducerProperties(
      serverProperties = serverProperties,
      tokenProvider = None,
      topic = topic
    )
    props.tokenProvider should === (None)
  }

  it should "also be able to set message send max retries" in {
    val props = ProducerProperties(
      serverProperties = serverProperties,
      tokenProvider = Option(() => token),
      topic = topic
    )

    props.serverProperties should === (serverProperties)
    props.tokenProvider.get.apply should === (token)
    props.topic should === (topic)
  }
}
