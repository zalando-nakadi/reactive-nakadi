package de.zalando.react.nakadi.client.providers

import java.io.ByteArrayOutputStream

import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.scaladsl.{Flow, Sink, Source}

import akka.util.ByteString
import akka.actor.{ActorRef, ActorContext}
import akka.event.LoggingAdapter
import akka.stream._

import de.zalando.react.nakadi.client._
import de.zalando.react.nakadi.{NakadiActorPublisher, ProducerProperties, ConsumerProperties}
import de.zalando.react.nakadi.client.models.EventStreamBatch

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


object ConsumeStatus {
  case object Start
  case object Init
  case object Acknowledge
  case object Complete
}


class ConsumeEvents(properties: ConsumerProperties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    outgoingConnection: Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]]) {

  import actorContext.dispatcher

  val DefaultBufferSize = 1024
  val DefaultParallelism = 100

  def stream(receiverActorRef: ActorRef)(implicit materializer: ActorMaterializer): Unit = {
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
      .withHeaders(
        headers.Authorization(OAuth2BearerToken(properties.tokenProvider.apply())),
        headers.Accept(MediaRange(`application/json`))
      )

    Source
      .single(request)
      .via(outgoingConnection)
      .runWith(Sink.foreachParallel(DefaultParallelism) {
        case HttpResponse(status, headers, entity, _) if status.isSuccess() =>
          import ConsumeStatus._
          entity
            .dataBytes
            .via(Flow[ByteString].map(parseJson).buffer(DefaultBufferSize, OverflowStrategy.backpressure))
            .runWith(Sink.actorRefWithAck(receiverActorRef, Init, Acknowledge, Complete))
        case HttpResponse(code, _, _, _) =>
          log.info(s"Request failed, response code: $code")
      }).map(_ => log.info("connection closed to [topic={}]", properties.topic)).recover {
        case err: StreamTcpException => log.error(err, s"Error connecting to Nakadi ${err.getMessage}")
        case ex => log.error(ex, "Error connecting to Nakadi")
      }
  }

  private def parseJson(byteString: ByteString) = {
    import spray.json._
    import JsonProtocol._

    var depth: Int = 0
    var hasOpenString: Boolean = false
    val bout = new ByteArrayOutputStream(1024)

    @tailrec
    def recur(byteString: ByteString): String = {
      // Cant rely on EOL because Nakadi can put it anywhere in the body
      bout.write(byteString.head.asInstanceOf[Int])
      byteString.head match {
        case '"' =>
          hasOpenString = !hasOpenString
          recur(byteString.tail)
        case '{' if !hasOpenString =>
          depth += 1
          recur(byteString.tail)
        case '}' if !hasOpenString =>
          depth -= 1
          if (depth == 0 && bout.size != 0) {
            val rawEvent = bout.toString()
            bout.reset()
            rawEvent
          } else {
            recur(byteString.tail)
          }
        case _ => recur(byteString.tail)
      }
    }

    Try(recur(byteString).parseJson.convertTo[EventStreamBatch]) match {
      case Success(event) => event
      case Failure(err) => log.error(err, "Issue decoding JSON")
    }
  }
}


class ProduceEvents(properties: ProducerProperties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    outgoingConnection: Flow[HttpRequest, HttpResponse, Future[OutgoingConnection]]) {

  import actorContext.dispatcher

  def publish(events: Seq[String], flowId: Option[String])(implicit materializer: ActorMaterializer): Unit = {
    val postEventUri = URI_POST_EVENTS.format(properties.topic)

    // FIXME - Need better way to handle this. Perhaps retries and / or return Future of success result
    events.foreach { event =>
      val uri = s"${properties.urlSchema}${properties.server}$postEventUri"
      val request = HttpRequest(uri = uri, method = POST)
        .withHeaders(headers.Authorization(OAuth2BearerToken(properties.tokenProvider.apply())))
        .withEntity(ContentType(`application/json`), event)

      Source
        .single(request)
        .via(outgoingConnection)
        .runWith(Sink.foreachParallel(100) {
          case HttpResponse(status, headers, entity, _) if status.isSuccess() =>
            val body = entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
            log.debug(s"Got response, body: $body")
          case HttpResponse(code, _, _, _) =>
            log.info(s"Request failed, response code: $code")
        }).recover {
          case err: StreamTcpException => log.error(err, s"Error connecting to Nakadi ${err.getMessage}")
          case ex => log.error(ex, "Error connecting to Nakadi")
        }
    }
  }
}
