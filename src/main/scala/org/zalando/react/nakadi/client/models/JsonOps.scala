package org.zalando.react.nakadi.client.models

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.data.validation.ValidationError

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.control.Exception.nonFatalCatch


object JsonOps {

  implicit val jodaDateTimeReads = Reads[DateTime] {
    _.validate[String].flatMap { dateStr =>
      nonFatalCatch.either(DateTime.parse(dateStr, ISODateTimeFormat.dateTime())).fold(
        ex => JsError(Seq(JsPath() -> Seq(ValidationError(ex.getMessage)))),
        JsSuccess(_)
      )
    }
  }

  implicit val readsMetaData: Reads[EventMetadata] = (
    (__ \ "eid").read[String] and
    (__ \ "event_type").readNullable[String] and
    (__ \ "occurred_at").read[DateTime] and
    (__ \ "received_at").readNullable[DateTime] and
    (__ \ "parent_eids").readNullable[Seq[String]] and
    (__ \ "flow_id").readNullable[String]
  )(EventMetadata)

  implicit val writesMetaData: Writes[EventMetadata] = (
    (__ \ "eid").write[String] and
    (__ \ "event_type").writeNullable[String] and
    (__ \ "occurred_at").write[String].contramap[DateTime](ISODateTimeFormat.dateTime().print) and
    (__ \ "received_at").writeNullable[String].contramap[Option[DateTime]](_.map(ISODateTimeFormat.dateTime().print)) and
    (__ \ "parent_eids").writeNullable[Seq[String]] and
    (__ \ "flow_id").writeNullable[String]
  )(unlift(EventMetadata.unapply))

  implicit val readsDataChangeEvent: Reads[DataChangeEvent] = (
    (__ \ "data_type").read[String] and
    (__ \ "data_op").read[String].map(DataOpEnum.apply) and
    (__ \ "data").read[EventPayload] and
    (__ \ "metadata").read[EventMetadata]
  )(DataChangeEvent)

  implicit val writesDataChangeEvent: Writes[DataChangeEvent] = (
    (__ \ "data_type").write[String] and
    (__ \ "data_op").write[String].contramap(DataOpEnum.contrapply) and
    (__ \ "data").write[EventPayload] and
    (__ \ "metadata").write[EventMetadata]
  )(unlift(DataChangeEvent.unapply))

  implicit val readsBusinessEvent: Reads[BusinessEvent] = (
    (__ \ "metadata").read[EventMetadata] and
    (__ \ 'metadata).json.prune
    )(BusinessEvent)

  implicit val writesBusinessEvent: Writes[BusinessEvent] = new Writes[BusinessEvent] {
    override def writes(o: BusinessEvent): JsValue = Json.obj(
      "metadata" -> o.metadata
    ) ++ o.payload
  }

  implicit val readsEvent: Reads[Event] = new Reads[Event] {
    override def reads(json: JsValue): JsResult[Event] = {
      if ((json \ "data").toOption.isDefined) {
        json.validate[DataChangeEvent]
      } else {
        json.validate[BusinessEvent]
      }
    }
  }

  implicit val writesEvent: Writes[Event] = new Writes[Event] {
    override def writes(o: Event): JsValue = o match {
      case be: BusinessEvent => Json.toJson(be)(writesBusinessEvent) //needed because it enters endless loop, possibly bug in play-json
      case dce: DataChangeEvent => Json.toJson(dce)(writesDataChangeEvent)
    }
  }

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

  implicit val readsEventTypeStatistics: Reads[EventTypeStatistics] = (
    (__ \ "expected_write_rate").readNullable[Long] and
    (__ \ "message_size").readNullable[Long] and
    (__ \ "read_parallelism").readNullable[Long] and
    (__ \ "write_parallelism").readNullable[Long]
  )(EventTypeStatistics)

  implicit val writesEventTypeStatistics: Writes[EventTypeStatistics] = (
    (__ \ "expected_write_rate").writeNullable[Long] and
    (__ \ "message_size").writeNullable[Long] and
    (__ \ "read_parallelism").writeNullable[Long] and
    (__ \ "write_parallelism").writeNullable[Long]
  )(unlift(EventTypeStatistics.unapply))

  implicit val readsEventType: Reads[EventType] = (
    (__ \ "name").read[String] and
    (__ \ "statistics").readNullable[EventTypeStatistics] and
    (__ \ "partition_key_fields").read[Seq[String]] and
    (__ \ "data_key_fields").readNullable[Seq[String]] and
    (__ \ "owning_application").read[String] and
    (__ \ "validation_strategies").readNullable[Seq[String]] and
    (__ \ "partition_resolution_strategy").readNullable[play.api.libs.json.JsValue] and
    (__ \ "schema").readNullable[play.api.libs.json.JsValue] and
    (__ \ "category").read[String].map(EventTypeCategoryEnum.apply) and
    (__ \ "enrichment_strategies").read[Seq[String]]
  )(EventType)

  implicit val writesEventType: Writes[EventType] = (
    (__ \ "name").write[String] and
    (__ \ "statistics").writeNullable[EventTypeStatistics] and
    (__ \ "partition_key_fields").write[Seq[String]] and
    (__ \ "data_key_fields").writeNullable[Seq[String]] and
    (__ \ "owning_application").write[String] and
    (__ \ "validation_strategies").writeNullable[Seq[String]] and
    (__ \ "partition_resolution_strategy").writeNullable[play.api.libs.json.JsValue] and
    (__ \ "schema").writeNullable[play.api.libs.json.JsValue] and
    (__ \ "category").write[String].contramap(EventTypeCategoryEnum.contrapply) and
    (__ \ "enrichment_strategies").write[Seq[String]]
  )(unlift(EventType.unapply))

}
