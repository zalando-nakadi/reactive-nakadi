package de.zalando.react.nakadi.client

import spray.json._

import models._


object NakadiJsonProtocol extends DefaultJsonProtocol {

  implicit object EventStreamBatchFormat extends RootJsonReader[EventStreamBatch] {

    private def handleCursor(cursor: Map[String, JsValue]): Cursor = {
      val cursorJsObj = JsObject(cursor.map { case (k, v) => k -> v.asInstanceOf[JsString]} )
      cursorJsObj.getFields("partition", "offset") match {
        case Seq(JsString(partition), JsString(offset)) => Cursor(partition = partition, offset = offset)
        case _ => throw new IllegalArgumentException("The partition key for the cursor needs to be set")
      }
    }

    override def read(value: JsValue) = {

      value.asJsObject.getFields("events", "cursor") match {
        case Seq(JsArray(events), JsObject(cursor)) =>
          EventStreamBatch(
            cursor = handleCursor(cursor),
            events = Option(events.map(_.compactPrint).toSeq)
          )
        case Seq(JsObject(cursor)) =>
          EventStreamBatch(
            cursor = handleCursor(cursor),
            events = None
          )
        case _ =>
          throw new IllegalArgumentException(s"Invalid JSON for ${value.compactPrint}")
      }
    }
  }

}
