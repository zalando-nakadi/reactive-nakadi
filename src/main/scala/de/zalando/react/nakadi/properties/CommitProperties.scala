package de.zalando.react.nakadi.properties

import scala.concurrent.duration._

case class CommitProperties(staleLeaseDelta: FiniteDuration,
                            awsCommitRegion: String,
                            awsDynamoDbReadCapacityUnits: Long,
                            awsDynamoDbWriteCapacityUnits: Long)

object CommitProperties {
  def apply: CommitProperties = {

    new CommitProperties(
      staleLeaseDelta = 10.minutes,
      awsCommitRegion = "eu-west-1",
      awsDynamoDbReadCapacityUnits = 5,
      awsDynamoDbWriteCapacityUnits = 6
    )
  }
}