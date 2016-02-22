package de.zalando.react.nakadi

import akka.actor.ActorSystem
import de.zalando.react.nakadi.client.NakadiClient


class ReactiveNakadiProducer(val properties: ProducerProperties, actorSystem: ActorSystem) {

  val nakadiClient = NakadiClient(properties, actorSystem)
}

object ReactiveNakadiProducer {

  def apply(properties: ProducerProperties, actorSystem: ActorSystem): ReactiveNakadiProducer = {
    new ReactiveNakadiProducer(properties, actorSystem)
  }
}
