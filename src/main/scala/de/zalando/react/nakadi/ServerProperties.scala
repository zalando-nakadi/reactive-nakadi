package de.zalando.react.nakadi

import scala.concurrent.duration._

case class ServerProperties(
  host: String,
  port: Int,
  isConnectionSSL: Boolean,
  acceptAnyCertificate: Boolean = false,
  connectionTimeout: FiniteDuration = 20.seconds
) {
  override def toString = {
    val secure = if (isConnectionSSL) "https" else "http"
    s"$secure://$host:$port"
  }
}
