package org.zalando.react.nakadi.properties

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


class ServerPropertiesSpec extends FlatSpec with Matchers with MockFactory {

  "ServerProperties" should "handle default case" in {

    val serverProperties = ServerProperties("localhost", 8080, isConnectionSSL = false)
    serverProperties.host should === ("localhost")
    serverProperties.port should === (8080)
    serverProperties.isConnectionSSL should === (false)
    serverProperties.connectionTimeout should === (20.seconds)
  }

  it should "override toString correctly when using plain text" in {
    val serverProperties = ServerProperties("some.host", 80, isConnectionSSL = false)
    serverProperties.toString should === ("http://some.host:80")
  }

  it should "override toString correctly when using ssl" in {
    val serverProperties = ServerProperties("some.host", 443, isConnectionSSL = true)
    serverProperties.toString should === ("https://some.host:443")
  }

}
