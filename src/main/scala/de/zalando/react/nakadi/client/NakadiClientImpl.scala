package de.zalando.react.nakadi.client

import java.io.ByteArrayOutputStream
import java.security.SecureRandom
import java.security.cert.X509Certificate

import javax.net.ssl.TrustManager
import javax.net.ssl.{X509TrustManager, SSLContext}

import akka.actor.{Actor, ActorLogging, Props}
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.{Http, HttpsContext}
import akka.stream.StreamTcpException
import akka.stream.scaladsl.ImplicitMaterializer
import akka.util.ByteString
import de.zalando.react.nakadi._
import de.zalando.react.nakadi.client.models.EventStreamBatch

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


object NakadiClientImpl {

  case object ListenForEvents
  case object PostEvent

  def props(consumerProperties: ConsumerProperties) = {
    Props(new NakadiClientImpl(consumerProperties))
  }
}


class NakadiClientImpl(val properties: ConsumerProperties) extends Actor
  with ImplicitMaterializer
  with ActorLogging
  with NakadiClient {

  import context.dispatcher

  def http = {
    val h = Http(context.system)

    properties.securedConnection match {
      case true =>
        val sslContext = if (properties.sslVerify) SSLContext.getDefault else {

          val permissiveTrustManager: TrustManager = new X509TrustManager() {
            override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
            override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
            override def getAcceptedIssuers(): Array[X509Certificate] = Array.empty
          }

          val ctx = SSLContext.getInstance("TLS")
          ctx.init(Array.empty, Array(permissiveTrustManager), new SecureRandom())
          ctx
        }
        h.outgoingConnectionTls(properties.server.toString, properties.port)
        h.setDefaultClientHttpsContext(HttpsContext(sslContext))
      case false =>
        h.outgoingConnection(properties.server.toString, properties.port)
    }
    h
  }

  override def receive: Receive = {
    case NakadiClientImpl.ListenForEvents =>
      listenForEvents()
  }

  override def postEvent(name: String, event: String, flowId: Option[String]): Future[Unit] = {
    val uri = URI_POST_EVENTS.format(name)

    val request = HttpRequest(uri = uri, method = POST)
      .withHeaders(headers.Authorization(OAuth2BearerToken(properties.tokenProvider.apply())))
      .withEntity(ContentType(`application/json`), event)

    http.singleRequest(request).map {
      case HttpResponse(status, headers, entity, _) if status.isSuccess() =>
        val body = entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
        log.debug(s"Got response, body: $body")
      case HttpResponse(code, _, _, _) =>
        log.info(s"Request failed, response code: $code")
    }.recover {
      case err: StreamTcpException => log.error(err, s"Error connecting to Nakadi ${err.getMessage}")
      case ex => log.error(ex, "Error connecting to Nakadi")
    }
  }

  override def listenForEvents(): Unit = {
    val postEventUri = URI_STREAM_EVENTS.format(
      properties.topic,
      properties.batchLimit,
      properties.batchFlushTimeoutInSeconds.length,
      properties.streamLimit,
      properties.streamTimeoutInSeconds.length,
      properties.streamKeepAliveLimit
    )

    val uri = s"${properties.urlSchema}${properties.server}$postEventUri"
    val request = HttpRequest(uri = uri)
      .withHeaders(headers.Authorization(OAuth2BearerToken(properties.tokenProvider.apply())),
        headers.Accept(MediaRange(`application/json`)))

    http.singleRequest(request).map {
      case HttpResponse(status, headers, entity, _) if status.isSuccess() =>
        consumeStream(entity)
      case HttpResponse(code, _, _, _) =>
        log.info(s"Request failed, response code: $code")
    }.recover {
      case err: StreamTcpException => log.error(err, s"Error connecting to Nakadi ${err.getMessage}")
      case ex => log.error(ex, "Error connecting to Nakadi")
    }
  }

  private def consumeStream(entity: ResponseEntity) = {
    /*
     * We can not simply rely on EOL for the end of each JSON object as
     * Nakadi puts the in the middle of the response body sometimes.
     * For this reason, we need to apply some very simple JSON parsing logic.
     *
     * See also http://json.org/ for string parsing semantics
     */
    import spray.json._
    import JsonProtocol._

    var depth: Int = 0
    var hasOpenString: Boolean = false
    val bout = new ByteArrayOutputStream(1024)

    entity.dataBytes.runForeach {
      byteString => { byteString.foreach { byteItem =>
        bout.write(byteItem.asInstanceOf[Int])

        if (byteItem == '"') hasOpenString = !hasOpenString
        else if (!hasOpenString && byteItem == '{') depth += 1
        else if (!hasOpenString && byteItem == '}') {
          depth -= 1

          if (depth == 0 && bout.size != 0) {
            val rawEvent = bout.toString()
            Try(rawEvent.parseJson.convertTo[EventStreamBatch]) match {
              case Success(event) =>
                log.debug(s"RAW EVENT: $event")
                context.system.eventStream.publish(event)
              case Failure(err) => log.error(err, "Issue decoding JSON")
            }
            bout.reset()
          }
        }
      }}
    }
  }
}
