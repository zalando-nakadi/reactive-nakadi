package de.zalando.react.nakadi.utils

import java.util.UUID


trait IdGenerator {

  def generate: String
}

object IdGenerator extends IdGenerator {

  def generate: String = UUID.randomUUID().toString
}
