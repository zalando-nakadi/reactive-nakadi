package de.zalando.react.nakadi.client.providers

import java.io.ByteArrayOutputStream

import spray.json._

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.Http.OutgoingConnection
import akka.http.scaladsl.model.HttpEntity.{ChunkStreamPart, Chunked}
import akka.http.scaladsl.model.headers.{OAuth2BearerToken, RawHeader}

import akka.stream._
import akka.util.ByteString
import akka.event.LoggingAdapter
import akka.actor.{ActorContext, ActorRef}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph}

import de.zalando.react.nakadi.client._
import de.zalando.react.nakadi.client.NakadiJsonProtocol._
import de.zalando.react.nakadi.client.models.{Cursor, EventStreamBatch}
import de.zalando.react.nakadi.{ConsumerProperties, ProducerProperties}

import scala.concurrent.Future
import scala.annotation.tailrec
import scala.concurrent.duration._


object ConsumeCommand {
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

  val DefaultBufferSize = 1000

  def request: Future[HttpRequest] = {
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

    cursorHeader.map(_.fold(request)(request.addHeader(_)))
  }

  def stream(receiverActorRef: ActorRef)(implicit materializer: ActorMaterializer): Future[Unit] = {
    import ConsumeCommand._

    val parse = Flow[ChunkStreamPart].map(parseJson)
    val buff = Flow[EventStreamBatch].buffer(DefaultBufferSize, OverflowStrategy.backpressure)
    val logger = Flow[EventStreamBatch].log("nakadi-event-stream")
    val out = Sink.actorRefWithAck(
      ref = receiverActorRef,
      onInitMessage = Init,
      ackMessage = Acknowledge,
      onCompleteMessage = Complete,
      onFailureMessage = x => x
    )

    val consumer = Flow[HttpResponse].map {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        log.info(s"Successfully connected to Nakadi on ${properties.urlSchema}${properties.server}/")
        RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val in = Chunked
            .fromData(entity.contentType, entity.dataBytes)
            .withoutSizeLimit()
            .chunks

          in ~> parse ~> buff ~> logger ~> out

          ClosedShape
        }).run()
      case HttpResponse(status, _, entity, _) =>
        entity.toStrict(5.seconds).map { d =>
          log.warning(s"Request failed, response code: $status. Response body: ${d.data.decodeString("UTF-8")}")
        }
    }

    request.flatMap { Source
      .single(_)
      .via(outgoingConnection)
      .via(consumer)
      .runWith(Sink.ignore)
      .map(_ => ())
      .recover {
        case err: StreamTcpException => log.error(err, s"Error connecting to Nakadi ${err.getMessage}")
        case ex => log.error(ex, "Error connecting to Nakadi")
      }
    }
  }

  private def parseJson(chunk: ChunkStreamPart): EventStreamBatch = {
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

    recur(chunk.data).parseJson.convertTo[EventStreamBatch]
  }

  private def toHeader(cursor: Cursor) = {
    log.info(s"Using offset ${cursor.offset} on partition ${cursor.partition} for topic '${properties.topic}'")
    RawHeader("X-Nakadi-Cursors", Seq(cursor).toJson.toString)
  }

  private def readFromCommitHandler: Future[Option[Cursor]] = {
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

  private def cursorHeader: Future[Option[RawHeader]] = {
    properties.offset.fold(readFromCommitHandler.map(_.map(toHeader))) { offset =>
      val cursor = Cursor(partition = properties.partition, offset = offset.value)
      Future.successful(Option(toHeader(cursor)))
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
        .runWith(Sink.foreach {
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
