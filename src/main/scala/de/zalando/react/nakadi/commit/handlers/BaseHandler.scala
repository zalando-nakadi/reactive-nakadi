package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.NakadiMessages.Topic
import de.zalando.react.nakadi.commit.OffsetTracking

trait BaseHandler {

  def commitSync(groupId: String, topic: Topic, offsets: Seq[OffsetTracking])

}
