package de.zalando.react.nakadi

package object client {

  val URI_POST_EVENTS = "/event-types/%s/events"
  val URI_STREAM_EVENTS = "/event-types/%s/events?batch_limit=%s&batch_flush_timeout=%s&stream_limit=%s&stream_timeout=%s&stream_keep_alive_limit=%s"
}
