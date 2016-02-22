package de.zalando.react.nakadi

import akka.actor.ActorSystem
import de.zalando.react.nakadi.client.NakadiClient


class ReactiveNakadiConsumer(val properties: ConsumerProperties, actorSystem: ActorSystem) {

  val nakadiClient = NakadiClient(properties, actorSystem)
}

object ReactiveNakadiConsumer {

  def apply(properties: ConsumerProperties, actorSystem: ActorSystem): ReactiveNakadiConsumer = {
    new ReactiveNakadiConsumer(properties, actorSystem)
  }
}
