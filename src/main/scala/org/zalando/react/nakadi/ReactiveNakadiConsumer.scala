package org.zalando.react.nakadi

import akka.actor.ActorSystem
import org.zalando.react.nakadi.client.NakadiClient
import org.zalando.react.nakadi.properties.ConsumerProperties


class ReactiveNakadiConsumer(val properties: ConsumerProperties, actorSystem: ActorSystem) {

  val nakadiClient = NakadiClient(properties, actorSystem)
}

object ReactiveNakadiConsumer {

  def apply(properties: ConsumerProperties, actorSystem: ActorSystem): ReactiveNakadiConsumer = {
    new ReactiveNakadiConsumer(properties, actorSystem)
  }
}
