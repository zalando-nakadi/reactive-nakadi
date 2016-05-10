package de.zalando.react.nakadi

import de.zalando.react.nakadi.utils.IdGenerator
import de.zalando.react.nakadi.commit.handlers.BaseLeaseManager
import de.zalando.react.nakadi.commit.{OffsetMap, OffsetTracking, EventTypePartition}

import org.scalatest.{FlatSpec, Matchers}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


class LeaseManagerSpec extends FlatSpec with Matchers with MockFactory with ScalaFutures {

  val leaseHolder = "some-test-lease-holder"
  val leaseId = "random-test-lease-id"
  val partitionId = "15"
  val groupId = "some-group-id"
  val eventType = "some-event-type"
  val timestamp = new DateTime(DateTimeZone.UTC)

  // Map of event-type-partition to offset count
  val offsetMap = OffsetMap(Map(EventTypePartition(eventType, partitionId).hash -> 10))

  val commitHandler = mock[BaseLeaseManager]
  val idGenerator = mock[IdGenerator]
  val offsetTracking = {
    OffsetTracking(
      partitionId = partitionId,
      checkpointId = "10",
      leaseHolder = leaseHolder,
      leaseCounter = Option(0),
      leaseTimestamp = now,
      leaseId = Option(leaseId)
    )
  }

  def createLeaseManager = new LeaseManagerImpl(leaseHolder, commitHandler, 100.seconds, None, idGenerator)
  def setupIdGenerator = (idGenerator.generate _).expects.returning(leaseId)


  "LeaseManager" should "create a new instance with a lease id property" in {
    setupIdGenerator

    val leaseManager = createLeaseManager
    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map.empty)
  }

  it should "be able to flush a commit, given nothing exists" in {
    setupIdGenerator

    (commitHandler.put(_: String, _: String, _: OffsetTracking))
      .expects(groupId, eventType, *)
      .returning(Future.successful(offsetTracking.copy(leaseCounter = Option(1))))

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, eventType, partitionId)
      .returning(Future.successful(None))

    val leaseManager = createLeaseManager
    leaseManager.flush(groupId, eventType, partitionId, offsetMap).futureValue should === (true)

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map(partitionId -> 1))
  }

  it should "be able to flush a commit, given a previous lease already exists" in {
    setupIdGenerator

    (commitHandler.put(_: String, _: String, _: OffsetTracking))
      .expects(groupId, eventType, *)
      .returning(Future.successful(offsetTracking.copy(leaseCounter = Option(2))))

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, eventType, partitionId)
      .returning(Future.successful(Some(offsetTracking.copy(leaseCounter = Option(1)))))

    val leaseManager = createLeaseManager
    leaseManager.flush(groupId, eventType, partitionId, offsetMap).futureValue should === (true)

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map(partitionId -> 2))
  }

  it should "return true for requestLease given nothing exist" in {
    setupIdGenerator

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, eventType, partitionId)
      .returning(Future.successful(None))

    val leaseManager = createLeaseManager
    leaseManager.requestLease(groupId, eventType, partitionId).futureValue should === (true)

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map.empty)
  }

  it should "return true for valid validation condition" in {
    setupIdGenerator

    val offset = offsetTracking.copy(leaseCounter = Option(2), leaseTimestamp = now.minus(400))

    val leaseManager = createLeaseManager
    leaseManager.counter(partitionId) = 2
    leaseManager.validate(offset) should === (true)
  }

  it should "return true for validation if lease time stamp is after now but counter is the same" in {
    setupIdGenerator

    val offset = offsetTracking.copy(leaseCounter = Option(2), leaseTimestamp = now.plus(400))

    val leaseManager = createLeaseManager
    leaseManager.counter(partitionId) = 2
    leaseManager.validate(offset) should === (true)
  }

  it should "return true for validation if time is before now, but counter differ (i.e. stale lease)" in {
    setupIdGenerator

    val offset = offsetTracking.copy(leaseCounter = Option(2), leaseTimestamp = now.minus(400))

    val leaseManager = createLeaseManager
    leaseManager.counter(partitionId) = 5
    leaseManager.validate(offset) should === (true)
  }

  it should "return false for validation if lease counters dont match lease time stamp is after now" in {
    setupIdGenerator

    val offset = offsetTracking.copy(leaseCounter = Option(2), leaseTimestamp = now.plus(400))

    val leaseManager = createLeaseManager
    leaseManager.counter(partitionId) = 5
    leaseManager.validate(offset) should === (false)
  }

  private def now = new DateTime(DateTimeZone.UTC)
}
