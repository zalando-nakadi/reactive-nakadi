# Reactive Streams for Nakadi

[![Build Status](https://travis-ci.org/zalando/reactive-nakadi.svg?branch=master)](https://travis-ci.org/zalando/reactive-nakadi) [![Coverage Status](https://coveralls.io/repos/github/zalando/reactive-nakadi/badge.svg?branch=master)](https://coveralls.io/github/zalando/reactive-nakadi?branch=master)

[Reactive Streams](http://www.reactive-streams.org) wrapper for [Nakadi](https://github.com/zalando/nakadi) is enspired by [Reactive Kafka](https://github.com/softwaremill/reactive-kafka). Reactive-Nakadi is built using Akka, Akka Http and Akka Streams while communicating with Nakadi's low level API. It acts as a consumer client for Nakadi providing an interface for consuming and publishing events, while making use of Akka's backpressure functionality. It also provides offset checkpointing and later to provide high level lease management across multiple partitions.

An important point to note is that it is a library that can only be used in an Akka environment. It currently makes use of Akka version `2.4.2`. Reactive-nakadi is not yet compatible with [Play](https://www.playframework.com/) version `2.5.x` for various backwards compatibility reasons, so for now it will only work with versions `2.4.x`.

**Note:** This project is still under heavy development, and it is likely that soon after release, subsequent releases may not be fully backward compatible. The upcoming release is just in alpha-phase.

##Getting Started

###Installation

*Coming soon...*

###Usage

Below are a set of examples on how to use reactive-nakadi. For the following examples it is assumed that you have imported and have within scope an implicit instance of an `ActorSystem` and `ActorMaterializer`, e.g.:

```scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

implicit val system = ActorSystem("reactive-nakadi")
implicit val materializer = ActorMaterializer()
```

####Consuming Messages from Nakadi

This example is the simplest use case, a consumer that consumes events from Nakadi:
```scala
import org.zalando.react.nakadi.properties._
import de.zalando.react.nakadi.ReactiveNakadi
import org.zalando.react.nakadi.commit.handlers.aws.DynamoDBCommitManager
import de.zalando.react.nakadi.NakadiMessages.{
  BeginOffset, ProducerMessage, StringConsumerMessage, StringProducerMessage
}

val nakadi = new ReactiveNakadi()
val server = ServerProperties(
  host = "192.168.99.100", port = 8080, isConnectionSSL = false
)

val publisher: Publisher[StringConsumerMessage] = nakadi.consume(ConsumerProperties(
  serverProperties = server,
  tokenProvider = None,
  eventType = "my-streaming-event-type",
  groupId = "experimental-group",
  partition = "0",
  commitHandler = DynamoDBCommitManager(system, CommitProperties.apply),
  offset = None  // If Offset left empty it will read from last commit
))

Source
  .fromPublisher(publisher)
  .map(someProcessingOfMessage)
  .to(Sink.ignore)
  .run()
```
From the above example you will note a couple of things. First is the optional `tokenProvider`. Just set it to some form of callable, such as `val tokenProvider = Option(() => "my-barer-token")`.

Second thing you will notice is the `commitHandler`. This is required, so it is important your application is authenticated with AWS. You can authenticate using the AWS environment variables, or `~/.aws/credentials`. Take a look at the AWS documentation for more examples.

The third parameter `offset` is optional, but if left empty it will try to read the latest commit from the commit handler.  If there are no commits available, it will read from the `BEGIN` offset value in Nakadi. Alternatively you can set it to `Option(Offset("120"))` or `Option(BeginOffset)`. Both of which can be imported from `import de.zalando.react.nakadi.NakadiMessages.{ BeginOffset, Offset }`

Finally the `partition` value *unfortunately* needs to be hard coded. This is later to be removed when Lease Management feature is complete. This means that you will need to create multiple instances of a publisher for each partition, until the lease management feature is complete.

####Publishing Messages to Nakadi

This example will build on the previous, say we want to consume messages, and then publish the resulting message to another event-type in Nakadi.

```scala
import org.zalando.react.nakadi.properties._
import de.zalando.react.nakadi.ReactiveNakadi
import org.zalando.react.nakadi.commit.handlers.aws.DynamoDBCommitManager
import de.zalando.react.nakadi.NakadiMessages.{
  BeginOffset, ProducerMessage, StringConsumerMessage, StringProducerMessage
}

val nakadi = new ReactiveNakadi()
val server = ServerProperties(
  host = "192.168.99.100", port = 8080, isConnectionSSL = false
)

val publisher: Publisher[StringConsumerMessage] = nakadi.consume(ConsumerProperties(
  serverProperties = server,
  tokenProvider = None,
  eventType = "my-streaming-event-type",
  groupId = "experimental-group",
  partition = "0",
  commitHandler = DynamoDBCommitManager(system, CommitProperties.apply),
  offset = None  // If Offset left empty it will read from last commit
))

val subscriber: Subscriber[StringProducerMessage] = nakadi.publish(ProducerProperties(
  serverProperties = server,
  tokenProvider = None,
  eventType = "destination-event-type"
))


Source
  .fromPublisher(publisher)
  .map(someProcessingOfMessage)
  .to(Sink.fromSubscriber(subscriber))
  .run()
```

####Committing Offsets
So say we are consuming messages, but we want to keep track of where we are on the stream, i.e. we want to keep track of the Nakadi offsets. So again, based from our first example we have:

```scala
import org.zalando.react.nakadi.properties._
import de.zalando.react.nakadi.ReactiveNakadi
import org.zalando.react.nakadi.commit.handlers.aws.DynamoDBCommitManager
import de.zalando.react.nakadi.NakadiMessages.{
  BeginOffset, ProducerMessage, StringConsumerMessage, StringProducerMessage
}

val nakadi = new ReactiveNakadi()
val server = ServerProperties(
  host = "192.168.99.100", port = 8080, isConnectionSSL = false
)

val publisher: Publisher[StringConsumerMessage] = nakadi.consumeWithOffsetSink(ConsumerProperties(
  serverProperties = server,
  tokenProvider = None,
  eventType = "my-streaming-event-type",
  groupId = "experimental-group",
  partition = "0",
  commitHandler = DynamoDBCommitManager(system, CommitProperties.apply),
  offset = None  // If Offset left empty it will read from last commit
))

Source
  .fromPublisher(publisher.publisher)
  .map(someProcessingOfMessage)
  .to(publisher.offsetCommitSink)
  .run()
```

This will periodically checkpoint the offset to DynamoDB. By default the commit interval is every 30 seconds, but this can be configured by setting `commitInterval` in the `ConsumerProperties`.

It is important to note that the message type `StringConsumerMessage` is sent all the way through the flow. In other words, in the above example, the `someProcessingOfMessage` must return the message so that it can be then picked up by the commit sink.

Reactive-nakadi will take care of creating the DynamoDB table if it does not exist. The name will come under the following format `reactive-nakadi-{event-type}-{groupId}`. It will contain a row per partition, on which each "primary key" is the partitionId.

###Tuning

`NakadiActorSubscriber` and `NakadiActorPublisher` have their own thread pools, configured in [reference.conf](https://github.com/zalando/reactive-nakadi/blob/master/src/main/resources/reference.conf).
You can tune them by overriding `nakadi-publisher-dispatcher.thread-pool-executor` and
`nakadi-subscriber-dispatcher.thread-pool-executor` in your own configuration file.
Alternatively you can provide your own dispatcher name. It can be passed to appropriate variants of factory methods in
`ReactiveNakadi`: `publish()`, `producerActor()`, `producerActorProps()` or `consume()`, `consumerActor()`, `consumerActorProps()`.


##Development

If you are interested in experimenting with source code, just clone the repo. To run the unit tests:
```bash
$ sbt clean test
```

To run integration tests with [Nakadi](https://github.com/zalando/nakadi), you need to state which IP address Docker is running on. If on a mac, this can be found out using `docker-machine ls`. Nakadi is cloned down, and it and all of its dependencies are run in Docker. Two containers are run, `local-storages` which contains Postgres, Kafka and Zookeeper. The other container is Nakadi itself:
```bash
$ export DOCKER_IP=127.0.0.1
$ sbt clean it:test
```

##TODO

There is still a lot of work to be done on this, the most important one being lease management. Some of the high level outstanding tasks include:

- [x] Persistence of consumer Cursor. Nakadi plans to support consumer commits in later high level API versions
- [x] Read checkpoint from cursor commits and continue streaming events from that point for a given partition
- [ ] Lease management (PR avilable [here](https://github.com/zalando/reactive-nakadi/pull/6))
  - [x] Refactor DynamoDB for conditional updates
  - [ ] Internal validation / keep alive checks for lease manager worker
  - [ ] Lease Coordinator to manage worker consumers and lease managers
  - [ ] Single registered consumer per topic group
  - [ ] DynamoDB tests
- [ ] Configurable connection retries to Nakadi
