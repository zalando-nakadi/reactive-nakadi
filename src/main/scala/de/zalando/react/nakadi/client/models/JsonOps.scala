package de.zalando.react.nakadi.client.models

import play.api.libs.json._
import play.api.libs.functional.syntax._


object JsonOps {

  implicit val readsCursor: Reads[Cursor] = (
    (__ \ "partition").read[String] and
    (__ \ "offset").read[String]
  )(Cursor)

  implicit val writesCursor: Writes[Cursor] = (
    (__ \ "partition").write[String] and
    (__ \ "offset").write[String]
  )(unlift(Cursor.unapply))

  implicit val readsEventStreamBatch: Reads[EventStreamBatch] = (
    (__ \ "cursor").read[Cursor] and
    (__ \ "events").readNullable[Seq[Event]]
  )(EventStreamBatch)

  implicit val writesEventStreamBatch: Writes[EventStreamBatch] = (
    (__ \ "cursor").write[Cursor] and
    (__ \ "events").writeNullable[Seq[Event]]
  )(unlift(EventStreamBatch.unapply))
}
