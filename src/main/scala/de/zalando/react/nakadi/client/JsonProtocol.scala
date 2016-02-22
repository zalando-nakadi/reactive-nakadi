package de.zalando.react.nakadi.client

import spray.json._

import models._


object JsonProtocol extends DefaultJsonProtocol {

  implicit object EventStreamBatchFormat extends RootJsonReader[EventStreamBatch] {

    override def read(value: JsValue) = {

      value.asJsObject.getFields("events", "cursor") match {
        case Seq(JsArray(events), JsObject(cursor)) =>
          EventStreamBatch(
            cursor = Option(Cursor(
              partition = cursor.get("partition").map(_.compactPrint).getOrElse(throw new IllegalArgumentException("The partition key for the cursor needs to be set")),
              offset = cursor.get("offset").map(_.compactPrint).getOrElse(throw new IllegalArgumentException("The offset key for the cursor needs to be set"))
            )),
            events = Option(events.map(_.compactPrint).toSeq)
          )
        case Seq(JsObject(cursor)) =>
          EventStreamBatch(
            cursor = Option(Cursor(
              partition = cursor.get("partition").map(_.compactPrint).getOrElse(throw new IllegalArgumentException("The partition key for the cursor needs to be set")),
              offset = cursor.get("offset").map(_.compactPrint).getOrElse(throw new IllegalArgumentException("The offset key for the cursor needs to be set"))
            )),
            events = None
          )
        case Seq(JsArray(events)) =>
          EventStreamBatch(
            cursor = None,
            events = Option(events.map(_.compactPrint).toSeq)
          )
        case _ =>
          throw new IllegalArgumentException(s"Invalid JSON for ${value.compactPrint}")
      }
    }
  }

}
