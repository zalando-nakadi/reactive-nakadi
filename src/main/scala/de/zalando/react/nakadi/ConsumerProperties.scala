package de.zalando.react.nakadi

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration


object ConsumerProperties {

  /**
    * Consumer Properties
    *
    * @param server The nakadi endpoint
    *
    * @param topic
    * The high-level API hides the details of brokers from the consumer and allows consuming off the cluster of machines
    * without concern for the underlying topology. It also maintains the state of what has been consumed. The high-level API
    * also provides the ability to subscribe to topics that match a filter expression (i.e., either a whitelist or a blacklist
    * regular expression).  This topic is a whitelist only but can change with re-factoring below on the filterSpec
    */
  def apply(
    server: String,
    port: Int,
    securedConnection: Boolean,
    tokenProvider: () => String,
    offset: String,
    topic: String
  ): ConsumerProperties = {
    new ConsumerProperties(server, port, securedConnection, tokenProvider, topic)
  }
}

case class ConsumerProperties(
  server: String,
  port: Int,
  securedConnection: Boolean,
  tokenProvider: () => String,
  topic: String,
  offset: String = "earliest",
  commitInterval: Option[FiniteDuration] = None,
  consumerTimeoutSec: FiniteDuration = 5.seconds,
  batchLimit: Int = 0,
  batchFlushTimeoutInSeconds: FiniteDuration = 0.seconds,
  streamLimit: Int = 0,
  streamTimeoutInSeconds: FiniteDuration = 0.seconds,
  streamKeepAliveLimit: Int = 0,
  pollParallelism: Int = 0,
  autoReconnect: Boolean = false
) {

  /**
    * Use custom interval for auto-commit or commit flushing on manual commit.
    */
  def commitInterval(time: FiniteDuration): ConsumerProperties =
    this.copy(commitInterval = Option(time))

  /**
    * Consumer Timeout
    * Throw a timeout exception to the consumer if no message is available for consumption after the specified interval
    */
  def consumerTimeoutSec(timeInSec: FiniteDuration): ConsumerProperties =
    this.copy(consumerTimeoutSec = timeInSec)

  def readFromEndOfStream(): ConsumerProperties =
    this.copy(offset = "latest")

}
