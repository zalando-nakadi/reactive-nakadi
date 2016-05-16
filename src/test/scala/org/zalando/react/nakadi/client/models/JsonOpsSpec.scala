package org.zalando.react.nakadi.client.models

import org.joda.time.DateTime
import org.scalatest.{Matchers, WordSpec}
import play.api.libs.json.{JsObject, JsString, JsValue, Json}


class JsonOpsSpec extends WordSpec with Matchers {

  import JsonOps._


  val businessEvent =
    """
      |{
      |  "metadata": {
      |    "eid": "7b7c7100-0559-11e6-a837-0800200c9a66",
      |    "occurred_at": "2016-04-01T13:42:16.000Z",
      |    "event_type": "order-created"
      |  },
      |  "id": "504c91de-17c9-46d8-81ee-55135084d696"
      |}
    """.stripMargin

  val dataChangeEvent =
    """
      |{
      |  "data_type": "order",
      |  "data_op": "C",
      |  "data": {"some": "payload"},
      |  "metadata": {
      |    "eid": "7b7c7100-0559-11e6-a837-0800200c9a66",
      |    "occurred_at": "2016-04-01T13:42:16.000Z",
      |    "event_type": "order-created"
      |  }
      |}
    """.stripMargin

  "Event JsonOps" must {
    "correctly determine business event" in {

      val json = Json.parse(businessEvent)
      val validated = json.asOpt[Event]
      validated match {
        case Some(BusinessEvent(_, payload)) =>
          payload shouldEqual JsObject(Seq("id" -> JsString("504c91de-17c9-46d8-81ee-55135084d696")))
        case _ =>
          fail("expected correctly parsed BusinessEvent")
      }
    }

    "correctly determine data change event" in {
      val json = Json.parse(dataChangeEvent)
      val validated = json.asOpt[Event]
      validated match {
        case Some(d :DataChangeEvent) =>
          d.data shouldEqual JsObject(Seq("some" -> JsString("payload")))
        case _ =>
          fail("expected correctly parsed DataChangeEvent")
      }
    }

    "successfully serialize and deserialize the same events" in {
      val dataEvent = Json.parse(dataChangeEvent).as[DataChangeEvent]
      val dataJson = Json.toJson(dataEvent)
      dataJson shouldEqual Json.parse(dataChangeEvent)

      val businessEvt = Json.parse(businessEvent).as[BusinessEvent]
      val businessJson = Json.toJson(businessEvt)
      businessJson shouldEqual Json.parse(businessEvent)
    }

  }

}
