package de.zalando.react.nakadi


package object commit {
  type Partition = String
  type Offset = Long
  type Offsets = Map[Partition, Offset]
}