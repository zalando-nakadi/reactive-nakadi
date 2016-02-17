package de.zalando.react.nakadi

import akka.actor.ActorSystem


object ReactiveNakadiConsumer {

  def apply(properties: ConsumerProperties, actorSystem: ActorSystem): ReactiveNakadiConsumer = {
    new ReactiveNakadiConsumer(properties, actorSystem)
  }
}

class ReactiveNakadiConsumer(properties: ConsumerProperties, actorSystem: ActorSystem) {

  val nakadiClient = NakadiClient(properties, actorSystem)
}
