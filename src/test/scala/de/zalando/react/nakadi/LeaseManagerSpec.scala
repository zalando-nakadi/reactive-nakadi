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
import scala.concurrent.ExecutionContext.Implicits.global


class LeaseManagerSpec extends FlatSpec with Matchers with MockFactory with ScalaFutures {

  val leaseHolder = "some-test-lease-holder"
  val leaseId = "random-test-lease-id"
  val partitionId = "some-partition"
  val groupId = "some-group-id"
  val topic = "some-topic"
  val timestamp = new DateTime(DateTimeZone.UTC)
  val config: Config = ConfigFactory.load()

  // Map of topic-partition to offset count
  val offsetMap = OffsetMap(Map(TopicPartition(topic, 15) -> 10))
  val offsetTracking = offsetMap.toCommitRequestInfo(leaseHolder, Some(leaseId), timestamp)

  val commitHandler = mock[BaseHandler]
  val idGenerator = mock[IdGenerator]

  def createLeaseManager = LeaseManager(leaseHolder, partitionId, commitHandler, config, idGenerator)
  def setupIdGenerator = (idGenerator.generate _).expects.returning(leaseId)

  "LeaseManager" should "create a new instance with a lease id property" in {
    setupIdGenerator

    val leaseManager = createLeaseManager
    leaseManager.leaseId should === (leaseId)
    leaseManager.partitionId should === (partitionId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (0)
  }

  it should "be able to create an entry for the commit handler, given the correct parameters passed" in {
    setupIdGenerator

    (commitHandler.put(_: String, _: String, _: Seq[OffsetTracking]))
      .expects(groupId, topic, *)
      .returning(Future.successful(()))

    val leaseManager = createLeaseManager
    leaseManager.commit(groupId, topic, offsetMap).futureValue should === (())

    leaseManager.leaseId should === (leaseId)
    leaseManager.partitionId should === (partitionId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (1)
  }

  it should "return true for isLeaseAvailable given a row does not exist" in {
    setupIdGenerator

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(None))

    val leaseManager = createLeaseManager
    leaseManager.isLeaseAvailable(groupId, topic).futureValue should === (true)

    leaseManager.leaseId should === (leaseId)
    leaseManager.partitionId should === (partitionId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (0)
  }

  it should "return true for isLeaseAvailable given there is a row and the counter matches and lease time is later than now" in {
    setupIdGenerator

    val currentOffsetTracking = offsetTracking.head.copy(leaseCounter = Some(0), leaseTimestamp = now.plus(600))
    (commitHandler.put(_: String, _: String, _: Seq[OffsetTracking]))
      .expects(groupId, topic, *)
      .returning(Future.successful(()))

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(Some(currentOffsetTracking)))

    val leaseManager = createLeaseManager
    leaseManager.isLeaseAvailable(groupId, topic).futureValue should === (true)

    leaseManager.leaseId should === (leaseId)
    leaseManager.partitionId should === (partitionId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (1)
  }

  it should "return false for isLeaseAvailable given there is a row and the counter matches but lease time is after now" in {
    setupIdGenerator

    val currentOffsetTracking = offsetTracking.head.copy(leaseCounter = Some(0), leaseTimestamp = now.minus(600))

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(Some(currentOffsetTracking)))

    val leaseManager = createLeaseManager
    leaseManager.isLeaseAvailable(groupId, topic).futureValue should === (false)

    leaseManager.leaseId should === (leaseId)
    leaseManager.partitionId should === (partitionId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (0)
  }

  it should "return false for isLeaseAvailable given there is a row and lease time is after now, but the counter does not matches" in {
    setupIdGenerator

    val osTracking = offsetTracking.head.copy(leaseCounter = Some(10), leaseTimestamp = now.plus(600))

    (commitHandler.get(_: String, _: String, _: String))
      .expects(groupId, topic, partitionId)
      .returning(Future.successful(Some(osTracking)))

    val leaseManager = createLeaseManager
    leaseManager.isLeaseAvailable(groupId, topic).futureValue should === (false)

    leaseManager.leaseId should === (leaseId)
    leaseManager.partitionId should === (partitionId)
    leaseManager.leaseHolder should === (leaseHolder)
    leaseManager.counter should === (0)
  }

  it should "return a new valid lease timeout" in {
    setupIdGenerator

    val leaseManager = new LeaseManagerImpl(leaseHolder, partitionId, commitHandler, config, idGenerator)
    leaseManager.newLeaseTimeout.isAfterNow should === (true)
    leaseManager.newLeaseTimeout.minusSeconds(600) should === (now)
  }

  private def now = new DateTime(DateTimeZone.UTC)
}
