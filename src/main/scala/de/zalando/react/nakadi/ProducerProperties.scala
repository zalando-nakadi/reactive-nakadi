package de.zalando.react.nakadi

import scala.concurrent.duration._


object ProducerProperties {

  def apply(server: String,
            securedConnection: Boolean,
            tokenProvider: () => String,
            topic: String): ProducerProperties = {
    if (securedConnection) {
      new ProducerProperties(server = server, securedConnection = securedConnection, tokenProvider = tokenProvider, topic = topic, port = 443, urlSchema = "https://")
    } else {
      new ProducerProperties(server = server, securedConnection = securedConnection, tokenProvider = tokenProvider, topic = topic, port = 80, urlSchema = "http://")
    }
  }

}

case class ProducerProperties(
  server: String,
  securedConnection: Boolean,
  tokenProvider: () => String,
  topic: String,
  port: Int = 80,
  retries: Option[Int] = None,
  acceptAnyCertificate: Boolean = true,
  urlSchema: String = "https://",
  connectionTimeout: Duration = 1000.milliseconds
) {

  def withPort(port: Int): ProducerProperties =
    this.copy(port = port)

  def withUrlSchema(urlSchema: String): ProducerProperties = {
    if (!Seq("http://", "https://").contains(urlSchema))
      throw new IllegalArgumentException("Must pass in valid schema of http:// or https://")
    else
      this.copy(urlSchema = urlSchema)
  }

  /**
    * messageSendMaxRetries
    * This property will cause the producer to automatically retry a failed send request.
    * This property specifies the number of retries when such failures occur. Note that
    * setting a non-zero value here can lead to duplicates in the case of network errors
    * that cause a message to be sent but the acknowledgment to be lost.
    */
  def messageSendMaxRetries(num: Int): ProducerProperties = {
    this.copy(retries = Option(num))
  }

}