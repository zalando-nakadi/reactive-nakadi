package org.zalando.react.nakadi

import akka.actor.ActorSystem
import org.zalando.react.nakadi.client.NakadiClient
import org.zalando.react.nakadi.properties.ProducerProperties


class ReactiveNakadiProducer(val properties: ProducerProperties, actorSystem: ActorSystem) {

  val nakadiClient = NakadiClient(properties, actorSystem)
}

object ReactiveNakadiProducer {

  def apply(properties: ProducerProperties, actorSystem: ActorSystem): ReactiveNakadiProducer = {
    new ReactiveNakadiProducer(properties, actorSystem)
  }
}
