package de.zalando.react.nakadi.leases

import scala.collection.immutable.HashMap
import scala.concurrent.Future


trait LeaseRenewer {

  def initialize: Future[Boolean]

  def renewLeases: Future[Boolean]

  def renewSingleLease[T](lease: T): Future[T]

  def getCurrentlyHeldLeases[T]: Future[HashMap[String, T]]

  def getCurrentlyHeldLease[T](groupId: String, topic: String, partitionId: String): Future[T]

}
