package de.zalando.react.nakadi.client.models

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.data.validation.ValidationError

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.control.Exception.nonFatalCatch


object JsonOps {

  implicit val jodaDateTimeReads = Reads[DateTime] {
    _.validate[String].flatMap { dateStr =>
      nonFatalCatch.either(new DateTime(dateStr)).fold(
        ex => JsError(Seq(JsPath() -> Seq(ValidationError(ex.getMessage)))),
        JsSuccess(_)
      )
    }
  }

  implicit val readsMetaData: Reads[MetaData] = (
    (__ \ "eid").read[String] and
    (__ \ "event_type").readNullable[String] and
    (__ \ "occurred_at").read[DateTime] and
    (__ \ "received_at").readNullable[DateTime] and
    (__ \ "parent_eids").readNullable[Seq[String]] and
    (__ \ "flow_id").readNullable[String]
  )(MetaData)

  implicit val writesMetaData: Writes[MetaData] = (
    (__ \ "eid").write[String] and
    (__ \ "event_type").writeNullable[String] and
    (__ \ "occurred_at").write[String].contramap[DateTime](ISODateTimeFormat.dateTime().print) and
    (__ \ "received_at").writeNullable[String].contramap[Option[DateTime]](_.map(ISODateTimeFormat.dateTime().print)) and
    (__ \ "parent_eids").writeNullable[Seq[String]] and
    (__ \ "flow_id").writeNullable[String]
  )(unlift(MetaData.unapply))

  implicit val readsEvent: Reads[Event] = (
    (__ \ "data_type").read[String] and
    (__ \ "data_op").read[String].map(DataOpEnum.apply) and
    (__ \ "data").read[EventPayload] and
    (__ \ "metadata").read[MetaData]
  )(Event)

  implicit val writesEvent: Writes[Event] = (
    (__ \ "data_type").write[String] and
    (__ \ "data_op").write[String].contramap(DataOpEnum.contrapply) and
    (__ \ "data").write[EventPayload] and
    (__ \ "metadata").write[MetaData]
  )(unlift(Event.unapply))

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
