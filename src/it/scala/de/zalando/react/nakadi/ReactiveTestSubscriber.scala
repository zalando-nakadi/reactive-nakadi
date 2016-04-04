package de.zalando.react.nakadi

import de.zalando.react.nakadi.NakadiMessages.StringConsumerMessage
import akka.stream.actor.{ActorSubscriber, ActorSubscriberMessage, WatermarkRequestStrategy}


class ReactiveTestSubscriber extends ActorSubscriber {

  protected def requestStrategy = WatermarkRequestStrategy(10)

  var elements: Vector[StringConsumerMessage] = Vector.empty

  def receive = {

    case ActorSubscriberMessage.OnNext(element) =>
      elements = elements :+ element.asInstanceOf[StringConsumerMessage]
    case "get elements" =>
      sender ! elements
  }
}