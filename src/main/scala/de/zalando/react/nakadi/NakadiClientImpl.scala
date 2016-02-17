package de.zalando.react.nakadi

import javax.net.ssl.SSLContext
import java.io.ByteArrayOutputStream

import akka.util.ByteString
import akka.http.scaladsl.{HttpsContext, Http}
import akka.http.scaladsl.model._
import akka.actor.{Props, ActorLogging, Actor}
import akka.stream.scaladsl.ImplicitMaterializer
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.MediaTypes.`application/json`

import scala.concurrent.Future


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
        h.setDefaultClientHttpsContext(HttpsContext(SSLContext.getDefault))
        h.outgoingConnectionTls(properties.server.toString, properties.port)
      case false =>
        h.outgoingConnection(properties.server.toString, properties.port)
    }
    h
  }

  override def receive: Receive = {
    case NakadiClientImpl.ListenForEvents =>
      listenForEvents()
  }

  override def postEvent(name: String, event: String, flowId: Option[String]): Future[Boolean] = {
    val uri = URI_POST_EVENTS.format(name)

    // FIXME - Proper unmarshalling for `Event`
    val request = HttpRequest(uri = uri, method = POST)
      .withHeaders(headers.Authorization(OAuth2BearerToken(properties.tokenProvider.apply())))
      .withEntity(ContentType(`application/json`), event)

    http.singleRequest(request).map {
      case HttpResponse(status, headers, entity, _) if status.isSuccess() =>
        log.debug(s"Got response, body: ${entity.dataBytes.runFold(ByteString(""))(_ ++ _)}")
        true
      case HttpResponse(code, _, _, _) =>
        log.info(s"Request failed, response code: $code")
        false
    }.recover {
      case ex => log.error(ex, "Error connecting to Nakadi"); false
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

    val request = HttpRequest(uri = postEventUri)
      .withHeaders(headers.Authorization(OAuth2BearerToken(properties.tokenProvider.apply())),
        headers.Accept(MediaRange(`application/json`)))

    http.singleRequest(request).map {
      case HttpResponse(status, headers, entity, _) if status.isSuccess() =>
        //log.debug("Got response, body: " + entity.dataBytes.runFold(ByteString(""))(_ ++ _))
        consumeStream(entity)
      case HttpResponse(code, _, _, _) =>
        log.info(s"Request failed, response code: $code")
    }.recover {
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
    println("consuming stream")
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
          println(bout.toString())
          bout.reset()
        }
      }}
    }
  }
}
