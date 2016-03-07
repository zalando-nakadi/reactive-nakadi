package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.commit._

trait BaseHandler {

  def commitSync(offsets: Offsets)

}
