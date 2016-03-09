package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.NakadiMessages.Cursor

trait BaseHandler {

  def commitSync(cursors: Seq[Cursor])

}
