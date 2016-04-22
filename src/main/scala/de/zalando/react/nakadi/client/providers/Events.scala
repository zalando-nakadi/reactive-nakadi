package de.zalando.react.nakadi.client.providers

import akka.stream._
import akka.util.ByteString
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.actor.{ActorContext, ActorRef}
import akka.stream.scaladsl.{Flow, Framing, Sink}
import de.zalando.react.nakadi.client._
import de.zalando.react.nakadi.client.models._
import de.zalando.react.nakadi.NakadiMessages.{EventTypeMessage, ProducerMessage}
import de.zalando.react.nakadi.{ConsumerProperties, ProducerProperties}
import play.api.libs.ws._
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.concurrent.duration.Duration


object ConsumeCommand {
  case object Start
  case object Init
  case object Acknowledge
  case object Complete
}


trait BaseProvider {
  def uri: String
  def request: Future[WSRequest]
}


class ConsumeEvents(properties: ConsumerProperties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    clientProvider: ClientProvider) extends BaseProvider {

  import actorContext.dispatcher

  override val uri = {
    val uri = s"${properties.server}${URI_STREAM_EVENTS.format(properties.topic)}"
    log.debug(s"Making GET request to: $uri")
    uri
  }

  override lazy val request: Future[WSRequest] = {

    val queryParams = Seq(
      "batch_limit" -> properties.batchLimit.toString,
      "stream_limit" -> properties.streamLimit.toString,
      "batch_flush_timeout" -> properties.batchFlushTimeoutInSeconds.length.toString,
      "stream_timeout" -> properties.streamTimeoutInSeconds.length.toString,
      "stream_keep_alive_limit" -> properties.streamKeepAliveLimit.toString
    )

    val headers = Seq(
      "Content-Type" -> ContentTypes.`application/json`.toString()
    ) ++ properties.tokenProvider.map(tok => "Authorization" -> s"Bearer ${tok.apply()}")

    val request = clientProvider.get
      .url(uri)
      .withQueryString(queryParams:_*)
      .withHeaders(headers:_*)
      .withRequestTimeout(Duration.Inf)

    cursorHeader.map(_.fold(request)(request.withHeaders(_)))
  }

  def stream(receiverActorRef: ActorRef)(implicit materializer: ActorMaterializer): Future[Unit] = {
    import ConsumeCommand._

    request.flatMap { r => log.debug(s"Headers: ${r.headers}. Params: ${r.queryString}"); r.stream() }.map { stream =>
      stream.headers.status match {
        case StatusCodes.OK.intValue =>
          log.info(s"Successfully connected to Nakadi on ${properties.server}")

          stream
            .body
            .via(Framing.delimiter(ByteString('\n'), Int.MaxValue))
            .via(Flow[ByteString].map(_.utf8String))
            .via(Flow[String].log("nakadi-event-stream"))
            .via(Flow[String].map(parse))
            .runWith(Sink.actorRefWithAck(
              ref = receiverActorRef,
              onInitMessage = Init,
              ackMessage = Acknowledge,
              onCompleteMessage = Complete,
              onFailureMessage = err => log.error(err, "Internal stream processing error")
            ))
        case errorCode =>
          stream
            .body
            .map(b => log.warning(s"Request failed for $uri, response code: $errorCode. Response: ${b.decodeString("UTF-8")}"))
            .runWith(Sink.ignore)
            .recover {
              case err => log.error(err, "There was an error while handling an invalid response code")
            }
      }
    }.map(_ => ()).recover {
      case err =>
        log.error(err, "Error handling Nakadi stream")
        sys.error("Error handling Nakadi stream")
    }
  }

  def parse(body: String): Option[EventStreamBatch] = {
    import models.JsonOps._
    Json.parse(body).validate[EventStreamBatch].fold(
      invalid = errors => {
        errors.foreach(err => log.warning(s"field: ${err._1}, errors: ${err._2} in raw body $body"))
        None
      },
      valid = b => Some(b)
    )
  }

  def toHeader(cursor: Cursor) = {
    import models.JsonOps._

    log.info(s"Using offset ${cursor.offset} on partition ${cursor.partition} for topic '${properties.topic}'")
    ("X-Nakadi-Cursors", Json.toJson(Seq(cursor)).toString)
  }

  def readFromCommitHandler: Future[Option[Cursor]] = {
    properties
      .commitHandler
      .readCommit(properties.groupId, properties.topic, properties.partition).map(_.map { offsetTracking =>
        Cursor(partition = offsetTracking.partitionId, offset = offsetTracking.checkpointId)
      }).recover {
        case ex =>
          log.error(ex, "There was an error reading back the commit.")
          sys.error("Commit read exception raised")
      }
  }

  def cursorHeader: Future[Option[(String, String)]] = {
    properties.offset.fold(readFromCommitHandler.map(_.map(toHeader))) { offset =>
      val cursor = Cursor(partition = properties.partition, offset = offset.value)
      Future.successful(Option(toHeader(cursor)))
    }
  }

}


class ProduceEvents(properties: ProducerProperties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    clientProvider: ClientProvider) extends BaseProvider {

  import actorContext.dispatcher

  override val uri = {
    val uri = s"${properties.server}${URI_POST_EVENTS.format(properties.topic)}"
    log.debug(s"Making POST request to: $uri")
    uri
  }

  override val request: Future[WSRequest] = {

    val headers = Seq(
      "Content-Type" -> ContentTypes.`application/json`.toString()
    ) ++ properties.tokenProvider.map(tok => "Authorization" -> s"Bearer ${tok.apply()}")

    Future.successful(clientProvider.get.url(uri).withHeaders(headers:_*))
  }

  def publish(producerMessage: ProducerMessage): Future[Boolean] = {
    import de.zalando.react.nakadi.client.models.JsonOps._

    request.flatMap { req =>
      // FIXME - for now just take the flow Id from the head.
      val flowId = producerMessage.eventRecords.headOption.flatMap(_.metadata.flow_id)
      val actualRequest = flowId.fold(req)(flow => req.withHeaders("X-Flow-Id" -> flow))

      actualRequest.post(Json.toJson(producerMessage.eventRecords)).map {
        case resp if resp.status == StatusCodes.OK.intValue => true
        case resp =>
          log.warning(s"Request failed for $uri, response code: ${resp.status}. Response: ${resp.body}")
          false
      }.recover {
        case ex =>
          log.error(ex, "Error while attempting to publish event")
          false
      }
    }
  }
}

class PostEventType(properties: Properties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    clientProvider: ClientProvider) extends BaseProvider {
  import actorContext.dispatcher

  override val uri = {
    val uri = s"${properties.server}$URI_POST_EVENT_TYPES"
    log.debug(s"Making POST request to: $uri")
    uri
  }

  override val request: Future[WSRequest] = {

    val headers = Seq(
      "Content-Type" -> ContentTypes.`application/json`.toString()
    ) ++ properties.tokenProvider.map(tok => "Authorization" -> s"Bearer ${tok.apply()}")

    Future.successful(clientProvider.get.url(uri).withHeaders(headers:_*))
  }

  def post(eventTypeMessage: EventTypeMessage): Future[Boolean] = {
    import de.zalando.react.nakadi.client.models.JsonOps._

    request.flatMap { req =>
      req.post(Json.toJson(eventTypeMessage.eventType)).map {
        case resp if resp.status == StatusCodes.Created.intValue => true
        case resp =>
          log.warning(s"Request failed for $uri, response code: ${resp.status}. Response: ${resp.body}")
          false
      }.recover {
        case ex =>
          log.error(ex, "Error while attempting to create event type")
          false
      }
    }
  }
}
