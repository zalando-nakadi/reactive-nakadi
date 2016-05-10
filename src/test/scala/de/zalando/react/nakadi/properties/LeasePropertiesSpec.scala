package de.zalando.react.nakadi.properties

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


class LeasePropertiesSpec extends FlatSpec with Matchers with MockFactory {

  "LeaseProperties" should "handle default case" in {

    val leaseProperties = CommitProperties.apply
    leaseProperties.staleLeaseDelta should === (10.minutes)
    leaseProperties.awsCommitRegion should === ("eu-west-1")
    leaseProperties.awsDynamoDbReadCapacityUnits should === (5L)
    leaseProperties.awsDynamoDbWriteCapacityUnits should === (6L)
  }

}
