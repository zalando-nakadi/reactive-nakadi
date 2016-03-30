package de.zalando.react.nakadi.client.providers

import akka.actor.ActorContext
import akka.stream.ActorMaterializer

import org.asynchttpclient.DefaultAsyncHttpClientConfig

import play.api.libs.ws.WSClient
import play.api.libs.ws.ahc.AhcWSClient


trait ClientProvider {
  def get: WSClient
}

class HttpClientProvider(actorContext: ActorContext,
                         server: String, port: Int,
                         sslVerify: Boolean)(implicit val materializer: ActorMaterializer) extends ClientProvider {

  override val get: AhcWSClient = {
    val builder = new DefaultAsyncHttpClientConfig
      .Builder()
      .setHandshakeTimeout(1000)
      .setAcceptAnyCertificate(sslVerify)
      .build()
    new AhcWSClient(builder)
  }

}
