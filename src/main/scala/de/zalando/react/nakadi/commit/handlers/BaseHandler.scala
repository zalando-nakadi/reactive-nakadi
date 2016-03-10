package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.NakadiMessages.{Topic, Cursor}

trait BaseHandler {

  def commitSync(groupId: String, topic: Topic, cursors: Seq[Cursor])

}
