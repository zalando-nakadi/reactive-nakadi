package de.zalando.react.nakadi.commit.handlers.aws

import org.scalatest.{Matchers, FlatSpec}


class AWSConfigSpec extends FlatSpec with Matchers {

  "AWSConfig" should "successfully load defaults" in {

    val config = AWSConfig()
    config.readCapacityUnits should === (5L)
    config.writeCapacityUnits should === (6L)
    config.region should === ("eu-west-1")
  }

  it should "successfully load using specified resource" in {

    val configSrc = "src/test/resources/reference.conf"
    val config = AWSConfig(Some(configSrc))
    config.readCapacityUnits should === (5L)
    config.writeCapacityUnits should === (6L)
    config.region should === ("eu-west-1")
  }
}
