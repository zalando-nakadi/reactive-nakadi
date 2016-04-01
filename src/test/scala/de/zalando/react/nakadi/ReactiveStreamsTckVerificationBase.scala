package de.zalando.react.nakadi


import akka.actor.ActorSystem
import org.scalatest.Suite

trait ReactiveStreamsTckVerificationBase extends Suite with NakadiTest {

  override implicit val system: ActorSystem = ActorSystem()
  val message = "foo"
}