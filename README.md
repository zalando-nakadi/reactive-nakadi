# Reactive Streams for Nakadi

[![Build Status](https://travis-ci.org/zalando/reactive-nakadi.svg?branch=master)](https://travis-ci.org/zalando/reactive-nakadi)

[Reactive Streams](http://www.reactive-streams.org) wrapper for [Nakadi](https://github.com/zalando/nakadi) is enspired by [Reactive Kafka](https://github.com/softwaremill/reactive-kafka). Reactive-Nakadi is built using Akka, Akka Http and Akka Streams while communicating with Nakadi's low level API.

## TODO
There is still a lot of work to be done on this, but some of the high level outstanding tasks include:
* [x] Persistence of consumer Cursor. Nakadi plans to support consumer commits in later high level API versions
* [x] Pass cursor as part of request header
* [ ] Configurable connection retries to Nakadi
* [ ] Lease management for low level API
  * [ ] Internal automatic partition assignment
  * [ ] Single registered consumer per topic group
* [ ] Integrate with Nakadi High Level API
* [ ] Implement Zookeeper commit handler
* [ ] Unit / integration tests
* [ ] More documentation
* [ ] Extend ConsumerProperties / ProducerProperties to use Akka config


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
    partition = "0",
    commitHandler = new DynamoDBHandler(system),
    offset = None,  // If Offset left empty it will read from last commit
    acceptAnyCertificate = true,
    port = 443
  ))

  val subscriber: Subscriber[ProducerMessage] = nakadi.publish(ProducerProperties(
    server = "some-nakadi-server",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "test-topic-uppercase",
    acceptAnyCertificate = true,
    port = 443
  ))

  Source
    .fromPublisher(publisher)
    .map(m => ProducerMessage(eventRecord = m.events.map(_.toUpperCase())))
    .to(Sink.fromSubscriber(subscriber))
    .run()

}
```

#### Manual Commit

In order to be able to achieve "at-least-once" delivery, you can use following API to obtain an additional Sink, where you can stream back messages that you processed. An underlying actor will periodically flush offsets of these messages as committed. **Note: Currently offsets are commited to DynamoDB. Later it will be extended to use ZooKeeper and Nakadi's own high level API**

```scala
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.ActorMaterializer

import com.typesafe.config.ConfigFactory
import de.zalando.react.nakadi.commit.handlers.aws.DynamoDBHandler
import de.zalando.react.nakadi.NakadiMessages.{Offset, ConsumerMessage}

object Example extends App {

  val token = "<some-token>"

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("reactive-nakadi")
  implicit val materializer = ActorMaterializer()

  val nakadi = new ReactiveNakadi()

  val publisher: PublisherWithCommitSink = nakadi.consume(ConsumerProperties(
    server = "some-nakadi-server",
    securedConnection = true,
    tokenProvider = () => token,
    topic = "test-topic",
    partition = "0",
    commitHandler = new DynamoDBHandler(system),
    offset = Some(Offset("300")),  // If Offset left empty it will read from last commit
    acceptAnyCertificate = true,
    port = 443
  ))

  Source
    .fromPublisher(publisher.publisher)
    .map(processMessage)
    .to(publisher.offsetCommitSink)
    .run()
}
```

## Tuning

NakadiActorSubscriber and NakadiActorPublisher have their own thread pools, configured in `application.conf`.
You can tune them by overriding `nakadi-publisher-dispatcher.thread-pool-executor` and
`nakadi-subscriber-dispatcher.thread-pool-executor` in your own `application.conf` file.
Alternatively you can provide your own dispatcher name. It can be passed to appropriate variants of factory methods in
`ReactiveNakadi`: `publish()`, `producerActor()`, `producerActorProps()` or `consume()`, `consumerActor()`, `consumerActorProps()`.

