package org.zalando.react.nakadi.client.providers

import akka.Done
import akka.stream._
import akka.util.ByteString
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.Query
import akka.actor.{ActorContext, ActorRef}
import akka.http.scaladsl.model.MediaTypes._
import akka.stream.scaladsl.{Flow, Framing, GraphDSL, RunnableGraph, Sink, Source}
import akka.http.scaladsl.model.headers.{OAuth2BearerToken, RawHeader}
import play.api.libs.json.Json
import org.zalando.react.nakadi.client.models._
import org.zalando.react.nakadi.client.models.JsonOps._
import org.zalando.react.nakadi.NakadiMessages.{EventTypeMessage, ProducerMessage}
import org.zalando.react.nakadi.properties.{ConsumerProperties, ProducerProperties}
import org.zalando.react.nakadi.client.{Properties, URI_POST_EVENTS, URI_POST_EVENT_TYPES, URI_STREAM_EVENTS}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal


object ConsumeCommand {
  case object Start
  case object Init
  case object Acknowledge
  case object Complete
}


trait BaseProvider {

  def uri: String

  def request: Future[HttpRequest]

  def handleInvalidResponse(entity: ResponseEntity, uri: String, status: String)
                           (logHandler: String => Unit)
                           (implicit mat: ActorMaterializer, ec: ExecutionContext) = {

    entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String).map { b =>
      logHandler(s"Request failed for $uri, response code: $status. Response: $b")
    } flatMap { _ =>
      Future.failed(new RuntimeException("Error with Nakadi response"))
    }
  }
}


class ConsumeEvents(properties: ConsumerProperties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    clientProvider: HttpClientProvider) extends BaseProvider {

  import actorContext.dispatcher

  override val uri = {
    val uri = s"${properties.serverProperties}${URI_STREAM_EVENTS.format(properties.eventType)}"
    log.debug(s"Making GET request to: $uri")
    uri
  }

  override lazy val request: Future[HttpRequest] = {

    val queryParams = Map(
      "batch_limit" -> properties.batchLimit.toString,
      "stream_limit" -> properties.streamLimit.toString,
      "batch_flush_timeout" -> properties.batchFlushTimeoutInSeconds.length.toString,
      "stream_timeout" -> properties.streamTimeoutInSeconds.length.toString,
      "stream_keep_alive_limit" -> properties.streamKeepAliveLimit.toString
    )

    val requestWithoutToken = HttpRequest(
      method = HttpMethods.GET, uri = Uri(uri).withQuery(Query(queryParams))
    ).withHeaders(
      headers.Accept(MediaRange(`application/json`))
    )

    val req = properties.tokenProvider.fold(requestWithoutToken) { tok =>
      requestWithoutToken.addHeader(headers.Authorization(OAuth2BearerToken(tok.apply())))
    }

    cursorHeader.map(_.fold(req)(req.addHeader(_)))
  }

  def stream(receiverActorRef: ActorRef)(implicit mat: ActorMaterializer): Future[Unit] = {
    import ConsumeCommand._

    val delimiter = Framing.delimiter(ByteString('\n'), Int.MaxValue)
    val stringify = Flow[ByteString].map(_.utf8String)
    val debug = Flow[String].log("nakadi-event-stream")
    val unmarshal = Flow[String].map(parse)
    val out = Sink.actorRefWithAck(
      ref = receiverActorRef,
      onInitMessage = Init,
      ackMessage = Acknowledge,
      onCompleteMessage = Complete,
      onFailureMessage = receiverActorRef ! _
    )

    val consumer = Flow[HttpResponse].mapAsync(1) {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        log.info(s"Successfully connected to Nakadi on ${properties.serverProperties}/")
        Future.successful {
          RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
            import GraphDSL.Implicits._
            val in = entity.dataBytes

            in ~> delimiter ~> stringify ~> debug ~> unmarshal ~> out

            ClosedShape
          }).run()
        }
      case HttpResponse(status, _, entity, _) =>
        handleInvalidResponse(entity, uri, status.value)(log.warning)
    }

    request.flatMap {
      Source
        .single(_)
        .via(clientProvider.connection)
        .via(consumer)
        .runWith(Sink.ignore)
        .map(_ => ())
        .recover {
          case NonFatal(err) =>
            log.error(err, s"Nakadi connection error: ${err.getMessage}")
            receiverActorRef ! err
        }
    }
  }

  def parse(body: String): Option[EventStreamBatch] = {

    Json.parse(body).validate[EventStreamBatch].fold(
      invalid = errors => {
        errors.foreach(err => log.warning(s"field: ${err._1}, errors: ${err._2} in raw body $body"))
        None
      },
      valid = b => Some(b)
    )
  }

  def toHeader(cursor: Cursor) = {

    log.info(s"Using offset ${cursor.offset} on partition ${cursor.partition} for eventType '${properties.eventType}'")
    RawHeader("X-Nakadi-Cursors", Json.toJson(Seq(cursor)).toString)
  }

  def readFromCommitHandler: Future[Option[Cursor]] = {
    properties
      .commitHandler
      .get(properties.groupId, properties.eventType, properties.partition).map(_.map { offsetTracking =>
        Cursor(partition = offsetTracking.partitionId, offset = offsetTracking.checkpointId)
      }).recover {
        case ex =>
          log.error(ex, "There was an error reading back the commit.")
          sys.error("Commit read exception raised")
      }
  }

  def cursorHeader: Future[Option[RawHeader]] = {
    properties.offset.fold(readFromCommitHandler.map(_.map(toHeader))) { offset =>
      val cursor = Cursor(partition = properties.partition, offset = offset.value)
      Future.successful(Option(toHeader(cursor)))
    }
  }

}


class ProduceEvents(properties: ProducerProperties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    clientProvider: HttpClientProvider)(implicit val mat: ActorMaterializer) extends BaseProvider {

  import actorContext.dispatcher

  override val uri = {
    val uri = s"${properties.serverProperties}${URI_POST_EVENTS.format(properties.eventType)}"
    log.debug(s"Making POST request to: $uri")
    uri
  }

  override val request: Future[HttpRequest] = {

    val requestWithoutToken = HttpRequest(
      method = HttpMethods.POST, uri = Uri(uri)
    ).withHeaders(
      headers.Accept(MediaRange(`application/json`))
    )

    val request = properties.tokenProvider.fold(requestWithoutToken) { tok =>
      requestWithoutToken.addHeader(headers.Authorization(OAuth2BearerToken(tok.apply())))
    }

    Future.successful(request)
  }

  def publish(producerMessage: ProducerMessage): Future[Unit] = {
    import org.zalando.react.nakadi.client.models.JsonOps._

    request.flatMap { r =>

      val request = r.withEntity(
        ContentType(`application/json`),
        Json.toJson(producerMessage.eventRecords).toString
      )

      // FIXME Just taking head option for now. Need confirmation on how this works with regard to Nakadi API
      val finalRequest = producerMessage.eventRecords.headOption.flatMap(_.metadata.flow_id).fold(request) { flow =>
        request.addHeader(RawHeader("X-Flow-Id", flow))
      }

      Source
        .single(finalRequest)
        .via(clientProvider.connection)
        .mapAsync(4) {
          case HttpResponse(status, _, _, _) if status.isSuccess() => Future.successful(Done)
          case HttpResponse(status, _, entity, _) => handleInvalidResponse(entity, uri, status.value)(log.warning)
        }.runWith(Sink.ignore)
        .recover { case NonFatal(err) => log.error(err, s"Nakadi connection error: ${err.getMessage}"); throw err }
        .map(_ => ())
    }
  }
}

class PostEventType(properties: Properties,
                    actorContext: ActorContext,
                    log: LoggingAdapter,
                    clientProvider: HttpClientProvider)(implicit val mat: ActorMaterializer) extends BaseProvider {
  import actorContext.dispatcher

  override val uri = {
    val uri = s"${properties.serverProperties}$URI_POST_EVENT_TYPES"
    log.debug(s"Making POST request to: $uri")
    uri
  }

  override val request: Future[HttpRequest] = {

    val requestWithoutToken = HttpRequest(
      method = HttpMethods.POST, uri = Uri(uri)
    ).withHeaders(
      headers.Accept(MediaRange(`application/json`))
    )

    val request = properties.tokenProvider.fold(requestWithoutToken) { tok =>
      requestWithoutToken.addHeader(headers.Authorization(OAuth2BearerToken(tok.apply())))
    }

    Future.successful(request)
  }

  def post(eventTypeMessage: EventTypeMessage): Future[Unit] = {
    import org.zalando.react.nakadi.client.models.JsonOps._

    request.flatMap { req =>
      val finalRequest = req.withEntity(
        ContentType(`application/json`),
        Json.toJson(eventTypeMessage.eventType).toString()
      )

      Source
        .single(finalRequest)
        .via(clientProvider.connection)
        .mapAsync(4) {
          case HttpResponse(status, _, _, _) if status.isSuccess() => Future.successful(Done)
          case HttpResponse(status, _, entity, _) => handleInvalidResponse(entity, uri, status.value)(log.warning)
        }.runWith(Sink.ignore)
        .recover { case NonFatal(err) => log.error(err, s"Nakadi connection error: ${err.getMessage}"); throw err }
        .map(_ => ())
    }
  }
}
