package de.zalando.react.nakadi.client.providers

import java.io.ByteArrayOutputStream

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.headers.OAuth2BearerToken

import akka.util.ByteString
import akka.actor.ActorContext
import akka.event.LoggingAdapter
import akka.stream.{ActorMaterializer, StreamTcpException}

import de.zalando.react.nakadi.client._
import de.zalando.react.nakadi.{ProducerProperties, ConsumerProperties}
import de.zalando.react.nakadi.client.models.EventStreamBatch

import scala.util.{Failure, Success, Try}


class ConsumeEvents(properties: ConsumerProperties, http: HttpExt, log: LoggingAdapter, actorContext: ActorContext) {

  import actorContext.dispatcher

  def stream()(implicit materializer: ActorMaterializer): Unit = {
    val streamEventUri = URI_STREAM_EVENTS.format(
      properties.topic,
      properties.batchLimit,
      properties.batchFlushTimeoutInSeconds.length,
      properties.streamLimit,
      properties.streamTimeoutInSeconds.length,
      properties.streamKeepAliveLimit
    )

    val uri = s"${properties.urlSchema}${properties.server}$streamEventUri"
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

  private def consumeStream(entity: ResponseEntity)(implicit materializer: ActorMaterializer) = {
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
                actorContext.system.eventStream.publish(event)
              case Failure(err) => log.error(err, "Issue decoding JSON")
            }
            bout.reset()
          }
        }
      }}
    }
  }
}


class ProduceEvents(properties: ProducerProperties, http: HttpExt, log: LoggingAdapter, actorContext: ActorContext) {

  import actorContext.dispatcher

  def publish(events: Seq[String], flowId: Option[String])(implicit materializer: ActorMaterializer): Unit = {
    val postEventUri = URI_POST_EVENTS.format(properties.topic)

    // FIXME - Need better way to handle this. Perhaps retries and / or return Future of success result
    events.foreach { event =>
      val uri = s"${properties.urlSchema}${properties.server}$postEventUri"
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
  }
}
