package de.zalando.react.nakadi

import de.zalando.react.nakadi.utils.IdGenerator
import de.zalando.react.nakadi.commit.handlers.BaseHandler
import de.zalando.react.nakadi.commit.{OffsetMap, OffsetTracking, TopicPartition}

import org.scalatest.{FlatSpec, Matchers}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.joda.time.{DateTime, DateTimeZone}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


class LeaseManagerSpec extends FlatSpec with Matchers with MockFactory with ScalaFutures {

  val leaseHolder = "some-test-lease-holder"
  val leaseId = "random-test-lease-id"
  val partitionId = "15"
  val groupId = "some-group-id"
  val topic = "some-topic"
  val timestamp = new DateTime(DateTimeZone.UTC)

  // Map of topic-partition to offset count
  val offsetMap = OffsetMap(Map(TopicPartition(topic, partitionId.toInt) -> 10))
  val offsetTracking = offsetMap.toCommitRequestInfo(leaseHolder, Some(leaseId), timestamp)

  val commitHandler = mock[BaseHandler]
  val idGenerator = mock[IdGenerator]

  def createLeaseManager = LeaseManager(leaseHolder, commitHandler, 100.seconds, idGenerator)
  def setupIdGenerator = (idGenerator.generate _).expects.returning(leaseId)

  "LeaseManager" should "create a new instance with a lease id property" in {
    setupIdGenerator

    val leaseManager = createLeaseManager
    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map.empty)
  }

  it should "be able to create an entry for the commit handler, given the correct parameters passed" in {
    setupIdGenerator

    (commitHandler.put(_: String, _: String, _: Seq[OffsetTracking]))
      .expects(groupId, topic, *)
      .returning(Future.successful(offsetTracking.map(_.copy(leaseCounter = Option(1)))))

    val leaseManager = createLeaseManager
    leaseManager.commit(groupId, topic, offsetMap).futureValue should === (())

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map(partitionId -> 1))
  }

  it should "return true for isLeaseAvailable given nothing exist" in {
    setupIdGenerator

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(None))

    val leaseManager = createLeaseManager
    leaseManager.isLeaseAvailable(groupId, topic, partitionId).futureValue should === (true)

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map.empty)
  }

  it should "return true for isLeaseAvailable given the partitonId doesnt exist" in {
    setupIdGenerator

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(None))

    val leaseManager = createLeaseManager
    leaseManager.counter("some-other-partition-id") = 10
    leaseManager.isLeaseAvailable(groupId, topic, partitionId).futureValue should === (true)

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map("some-other-partition-id" -> 10))
  }

  it should "return true for isLeaseAvailable given the counter matches and lease time is later than now" in {
    setupIdGenerator

    val currentOffsetTracking = offsetTracking.head.copy(leaseCounter = Some(10), leaseTimestamp = now.plus(600))
    (commitHandler.put(_: String, _: String, _: Seq[OffsetTracking]))
      .expects(groupId, topic, *)
      .returning(Future.successful(Seq(currentOffsetTracking)))

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(Some(currentOffsetTracking)))

    val leaseManager = createLeaseManager
    leaseManager.counter(partitionId) = 10
    leaseManager.isLeaseAvailable(groupId, topic, partitionId).futureValue should === (true)

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map(partitionId -> 10))
  }

  it should "return false for isLeaseAvailable given the counter matches but lease time is after now" in {
    setupIdGenerator

    val currentOffsetTracking = offsetTracking.head.copy(leaseCounter = Some(10), leaseTimestamp = now.minus(600))

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(Some(currentOffsetTracking)))

    val leaseManager = createLeaseManager
    leaseManager.counter(partitionId) = 10
    leaseManager.isLeaseAvailable(groupId, topic, partitionId).futureValue should === (false)

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map(partitionId -> 10))
  }

  it should "return false for isLeaseAvailable given lease time is after now, but the counter does not match" in {
    setupIdGenerator

    val osTracking = offsetTracking.head.copy(leaseCounter = Some(10), leaseTimestamp = now.plus(600))

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(Some(osTracking)))

    val leaseManager = createLeaseManager
    leaseManager.counter(partitionId) = 5
    leaseManager.isLeaseAvailable(groupId, topic, partitionId).futureValue should === (false)

    leaseManager.leaseId should === (leaseId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (Map(partitionId -> 5))
  }

  private def now = new DateTime(DateTimeZone.UTC)
}
