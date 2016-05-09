package de.zalando.react.nakadi.properties

import scala.concurrent.duration._

case class LeaseProperties(staleLeaseDelta: FiniteDuration,
                           awsCommitRegion: String,
                           awsDynamoDbReadCapacityUnits: Long,
                           awsDynamoDbWriteCapacityUnits: Long)

object LeaseProperties {
  def apply: LeaseProperties = {

    new LeaseProperties(
      staleLeaseDelta = 10.minutes,
      awsCommitRegion = "eu-west-1",
      awsDynamoDbReadCapacityUnits = 5,
      awsDynamoDbWriteCapacityUnits = 6
    )
  }
}