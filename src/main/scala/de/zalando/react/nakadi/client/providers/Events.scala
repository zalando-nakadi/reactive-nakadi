package de.zalando.react.nakadi.client.providers

import akka.stream._
import akka.util.ByteString
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.actor.{ActorContext, ActorRef}
import akka.stream.scaladsl.{Flow, Framing, Sink}

import de.zalando.react.nakadi.client._
import de.zalando.react.nakadi.client.models._
import de.zalando.react.nakadi.NakadiMessages.ProducerMessage
import de.zalando.react.nakadi.{ConsumerProperties, ProducerProperties}

import play.api.libs.ws._
import play.api.libs.json. Json

import scala.concurrent.Future
import scala.concurrent.duration.Duration


object ConsumeCommand {
  case object Start
  case object Init
  case object Acknowledge
  case object Complete
}


class ConsumeEvents(properties: ConsumerProperties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    clientProvider: ClientProvider) {

  import actorContext.dispatcher

  def request: Future[WSRequest] = {
    val streamEventUri = URI_STREAM_EVENTS.format(properties.topic)

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
    println(headers)

    val request = clientProvider.get
      .url(s"${properties.server}$streamEventUri")
      .withQueryString(queryParams:_*)
      .withHeaders(headers:_*)
      .withRequestTimeout(Duration.Inf)

    log.debug(s"Request uri: ${request.uri.toString}")

    cursorHeader.map(_.fold(request)(request.withHeaders(_)))
  }

  def stream(receiverActorRef: ActorRef)(implicit materializer: ActorMaterializer): Future[Unit] = {
    import ConsumeCommand._

    request.flatMap(_.stream()).map { stream =>
      stream.headers.status match {
        case StatusCodes.OK.intValue =>
          log.info(s"Successfully connected to Nakadi on $${properties.server}/")

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
            .map(b => log.warning(s"Request failed, response code: $errorCode. Response: ${b.decodeString("UTF-8")}"))
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
                    clientProvider: ClientProvider) {

  import actorContext.dispatcher

  def request: WSRequest = {
    val postEventUri = URI_POST_EVENTS.format(properties.topic)

    val headers = Seq(
      "Content-Type" -> ContentTypes.`application/json`.toString()
    ) ++ properties.tokenProvider.map(tok => "Authorization" -> s"Bearer ${tok.apply()}")

    clientProvider.get
      .url(s"${properties.server}$postEventUri")
      .withHeaders(headers:_*)
  }

  def publish(producerMessage: ProducerMessage): Future[Boolean] = {
    import de.zalando.react.nakadi.client.models.JsonOps._

    // FIXME - for now just take the flow Id from the head.
    val flowId = producerMessage.eventRecords.headOption.flatMap(_.metadata.flow_id)
    val req = flowId.fold(request)(flow => request.withHeaders("X-Flow-Id" -> flow))

    req.withMethod("POST").withBody(Json.toJson(producerMessage.eventRecords)).execute().map {
      case resp if resp.status == StatusCodes.OK.intValue => true
      case resp =>
        log.warning(s"Request failed, response code: ${resp.status}. Response: ${resp.body}")
        false
    }.recover {
      case ex =>
        log.error(ex, "Error while attempting to publish event")
        false
    }
  }
}
