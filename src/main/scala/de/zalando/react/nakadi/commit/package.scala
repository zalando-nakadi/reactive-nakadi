package de.zalando.react.nakadi


package object commit {
  type Offset = Long
  type Offsets = Map[TopicPartition, Offset]
}