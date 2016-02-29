package de.zalando.react.nakadi.client.providers

import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}

import akka.actor.ActorContext
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.model.{HttpResponse, HttpRequest}
import akka.http.scaladsl.{HttpsConnectionContext, Http}
import akka.stream.scaladsl.Flow

import scala.concurrent.Future


class HttpProvider(actorContext: ActorContext, server: String, port: Int, securedConnection: Boolean, sslVerify: Boolean) {

  def outgoingConnection: Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]] = {
    val h = Http(actorContext.system)

    securedConnection match {
      case true =>
        val sslContext = if (sslVerify) SSLContext.getDefault else {

          val permissiveTrustManager: TrustManager = new X509TrustManager() {
            override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
            override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
            override def getAcceptedIssuers(): Array[X509Certificate] = Array.empty
          }

          val ctx = SSLContext.getInstance("TLS")
          ctx.init(Array.empty, Array(permissiveTrustManager), new SecureRandom())
          ctx
        }
        h.outgoingConnectionHttps(server, port, new HttpsConnectionContext(sslContext))
      case false =>
        h.outgoingConnection(server.toString, port)
    }
  }

}
