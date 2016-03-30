package de.zalando.react.nakadi

import de.zalando.react.nakadi.NakadiMessages._
import de.zalando.react.nakadi.commit.handlers.BaseHandler

import scala.language.postfixOps
import scala.concurrent.duration._


object ConsumerProperties {

  def apply(
    server: String,
    securedConnection: Boolean,
    tokenProvider: () => String,
    topic: String,
    groupId: String,
    partition: String,
    commitHandler: BaseHandler
  ): ConsumerProperties = {
    if (securedConnection) { new ConsumerProperties(
      server = server,
      securedConnection = securedConnection,
      tokenProvider = tokenProvider,
      topic = topic,
      groupId = groupId,
      partition = partition,
      commitHandler = commitHandler,
      port = 443,
      urlSchema = "https://"
    )} else { new ConsumerProperties(
      server = server,
      securedConnection = securedConnection,
      tokenProvider = tokenProvider,
      topic = topic,
      groupId = groupId,
      partition = partition,
      commitHandler = commitHandler,
      port = 80,
      urlSchema = "http://"
    )}
  }
}

case class ConsumerProperties(
  server: String,
  securedConnection: Boolean,
  tokenProvider: () => String,
  topic: String,
  groupId: String,
  partition: String,
  commitHandler: BaseHandler,
  port: Int = 80,
  offset: Option[Offset] = None,
  commitInterval: FiniteDuration = 30.seconds,
  connectionTimeout: FiniteDuration = 1000.milliseconds,
  batchLimit: Int = 0,
  batchFlushTimeoutInSeconds: FiniteDuration = 30.seconds,
  streamLimit: Int = 0,
  streamTimeoutInSeconds: FiniteDuration = 0.seconds,
  streamKeepAliveLimit: Int = 0,
  pollParallelism: Int = 0,
  autoReconnect: Boolean = false,
  acceptAnyCertificate: Boolean = true,
  urlSchema: String = "https://"
) {

  /**
    * Use custom interval for auto-commit or commit flushing on manual commit.
    */
  def commitInterval(time: FiniteDuration): ConsumerProperties =
    this.copy(commitInterval = time)

  def readFromStartOfStream(): ConsumerProperties =
    this.copy(offset = Some(BeginOffset))

  def withPort(port: Int): ConsumerProperties =
    this.copy(port = port)

  def withUrlSchema(urlSchema: String): ConsumerProperties = {
    if (!Seq("http://", "https://").contains(urlSchema))
      throw new IllegalArgumentException("Must pass in valid schema of http:// or https://")
    else
      this.copy(urlSchema = urlSchema)
  }

}
