# Reactive Streams for Nakadi

[Reactive Streams](http://www.reactive-streams.org) wrapper for [Nakadi](https://github.com/zalando/nakadi) is enspired by [Reactive Kafka](https://github.com/softwaremill/reactive-kafka). Reactive-Nakadi is built using Akka, Akka Http and Akka Streams while communicating with Nakadi's low level API.

## TODO
There is still a lot of work to be done on this, but some of the high level outstanding tasks include:
- Configurable connection retries to Nakadi
- Persistence of consumer Cursor. Nakadi plans to support consumer commits in later high level API versions
- Unit / integration tests
- Some documentation


## Example Usage

#### Scala
```scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import com.typesafe.config.ConfigFactory

import de.zalando.react.nakadi.ReactiveNakadi
import de.zalando.react.nakadi.ConsumerProperties
import de.zalando.react.nakadi.ProducerProperties
import de.zalando.react.nakadi.NakadiMessages.ProducerMessage

object Example extends App {

  val token = "<some-token>"

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()

  val publisher: Publisher[ConsumerMessage] = nakadi.consume(ConsumerProperties(
    server = "some-nakadi-server",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "test-topic",
    sslVerify = false,
    port = 443
  ))

  val subscriber: Subscriber[ProducerMessage] = nakadi.publish(ProducerProperties(
    server = "some-nakadi-server",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "test-topic-uppercase",
    sslVerify = false,
    port = 443
  ))

  Source
    .fromPublisher(publisher)
    .map(m => ProducerMessage(eventRecord = m.events.map(_.toUpperCase())))
    .to(Sink.fromSubscriber(subscriber))
    .run()

}
```

## Tuning

NakadiActorSubscriber and NakadiActorPublisher have their own thread pools, configured in `application.conf`.
You can tune them by overriding `nakadi-publisher-dispatcher.thread-pool-executor` and
`nakadi-subscriber-dispatcher.thread-pool-executor` in your own `application.conf` file.
Alternatively you can provide your own dispatcher name. It can be passed to appropriate variants of factory methods in
`ReactiveNakadi`: `publish()`, `producerActor()`, `producerActorProps()` or `consume()`, `consumerActor()`, `consumerActorProps()`.

