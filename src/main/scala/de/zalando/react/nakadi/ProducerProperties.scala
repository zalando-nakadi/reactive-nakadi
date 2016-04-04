package de.zalando.react.nakadi

import scala.concurrent.duration._


object ProducerProperties {

  def apply(server: String, tokenProvider: Option[() => String], topic: String): ProducerProperties = {
    new ProducerProperties(server, tokenProvider, topic)
  }

}

case class ProducerProperties(
  server: String,
  tokenProvider: Option[() => String],
  topic: String,
  retries: Option[Int] = None,
  acceptAnyCertificate: Boolean = true,
  connectionTimeout: Duration = 1000.milliseconds
) {

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