package de.zalando.react.nakadi.client.providers

import akka.actor.ActorContext
import akka.stream.ActorMaterializer
import org.asynchttpclient.DefaultAsyncHttpClientConfig

import play.api.libs.ws.WSClient
import play.api.libs.ws.ahc.AhcWSClient

import scala.concurrent.duration._
import scala.concurrent.duration.Duration


trait ClientProvider {
  def get: WSClient
}

class HttpClientProvider(server: String,
                         port: Int,
                         connectionTimeout: Duration = 5000.milliseconds,
                         acceptAnyCertificate: Boolean = false)(implicit val materializer: ActorMaterializer) extends ClientProvider {

  override val get: AhcWSClient = {
    val builder = new DefaultAsyncHttpClientConfig
      .Builder()
      .setHandshakeTimeout(connectionTimeout.length.toInt)
      .setAcceptAnyCertificate(acceptAnyCertificate)

      // Bit of a hack - set the read time out to the max
      // so it does not stop consuming data from the stream
      // If we want to set a time out on the stream, we should
      // use set the relevant Nakadi parameter.
      .setReadTimeout(Int.MaxValue)
      .build()
    new AhcWSClient(builder)
  }

}
