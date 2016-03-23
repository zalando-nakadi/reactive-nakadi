package de.zalando.react.nakadi.commit.handlers.aws

import akka.actor.ActorSystem
import de.zalando.react.nakadi.commit.OffsetTracking

import de.zalando.react.nakadi.NakadiMessages.Topic
import de.zalando.react.nakadi.commit.handlers.BaseHandler

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.document.{Item, Table}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import org.joda.time.{DateTimeZone, DateTime}

import scala.concurrent.Future
import scala.collection.JavaConverters._


class DynamoDBHandler(system: ActorSystem, awsConfig: Option[AWSConfig] = None, clientProvider: Option[ClientProvider] = None) extends BaseHandler {

  import system.dispatcher

  val PartitionIdKey = "partitionId"
  val CheckpointIdKey = "checkpointId"
  val LeaseHolderKey = "leaseHolder"
  val LeaseCounterKey = "leaseCounter"
  val LeaseTimestampKey = "leaseTimestamp"
  val LeaseIdKey = "leaseId"

  private val log = system.log
  private lazy val awsConfiguration: AWSConfig = awsConfig.fold(AWSConfig())(cnf => cnf)
  private lazy val ddbClient = clientProvider.fold(ClientProvider(awsConfiguration.region))(provider => provider).client
  private val keySchema = Seq(new KeySchemaElement().withAttributeName(PartitionIdKey).withKeyType(KeyType.HASH))
  private val attributeDefinitions = Seq(new AttributeDefinition().withAttributeName(PartitionIdKey).withAttributeType(ScalarAttributeType.S))

  def tableName(groupId: String, topic: Topic) = s"reactive-nakadi-$topic-$groupId"

  override def commitSync(groupId: String, topic: Topic, offsets: Seq[OffsetTracking]): Future[Unit] = {
    put(groupId, topic, offsets)
  }

  override def readCommit(groupId: String, topic: Topic, partitionId: String): Future[Option[OffsetTracking]] = Future {

    Option(ddbClient.getTable(tableName(groupId, topic)).getItem(PartitionIdKey, partitionId)).map { i =>
      OffsetTracking(
        partitionId = i.getString(PartitionIdKey),
        checkpointId = i.getString(CheckpointIdKey),
        leaseHolder = i.getString(LeaseHolderKey),
        leaseCounter = Option(i.getLong(LeaseCounterKey)),
        leaseTimestamp = new DateTime(i.getString(LeaseTimestampKey), DateTimeZone.UTC),
        leaseId = Option(i.getString(LeaseIdKey))
      )
    }
  }

  def put(groupId: String, topic: Topic, offsets: Seq[OffsetTracking]): Future[Unit] = {

    withTable(groupId, topic, offsets) {
      case (table, true) =>
        // Table just created
        handleOnCreated(table, groupId, topic, offsets)
      case (table, false) =>
        // Table already existed
        handleUpdate(table, groupId, topic, offsets)
    }
  }

  private def handleUpdate(table: Table, groupId: String, topic: Topic, offsets: Seq[OffsetTracking]): Future[Unit] = {
    Future {
      offsets.map { offsetTracking =>
        val valueMap = new ValueMap()
          .withString(":cidval", offsetTracking.checkpointId)
          .withString(":lhval", offsetTracking.leaseHolder)
          .withNumber(":lcval", 1)
          .withString(":ltsval", offsetTracking.leaseTimestamp.toDateTime.toString)

        var leaseIdKey = ""
        offsetTracking.leaseId.foreach { leaseId =>
          valueMap.withString(":lidval", leaseId)
          leaseIdKey = s", $LeaseIdKey = :lidval"
        }

        table.updateItem(new UpdateItemSpec()
          .withPrimaryKey("partitionId", offsetTracking.partitionId)
          .withUpdateExpression(
            s"""
               |SET
               | $CheckpointIdKey = :cidval,
               | $LeaseHolderKey = :lhval,
               | $LeaseCounterKey = leaseCounter + :lcval,
               | $LeaseTimestampKey = :ltsval $leaseIdKey
               | """.stripMargin)
          .withValueMap(valueMap))
      }
    }.map(_.foreach(outcome => log.debug(s"Update item outcome: ${outcome.getUpdateItemResult}")))
  }

  private def handleOnCreated(table: Table, groupId: String, topic: Topic, offsets: Seq[OffsetTracking]): Future[Unit] = {

    Future {
      offsets.map { offsetTracking =>
        val item = new Item()
          .withPrimaryKey(PartitionIdKey, offsetTracking.partitionId)
          .withString(CheckpointIdKey, offsetTracking.checkpointId)
          .withString(LeaseHolderKey, offsetTracking.leaseHolder)
          .withNumber(LeaseCounterKey, 1)
          .withString(LeaseTimestampKey, offsetTracking.leaseTimestamp.toDateTime.toString)
        offsetTracking.leaseId.map(item.withString(LeaseIdKey, _))
        table.putItem(item)
      }
    }.map(_.foreach(outcome => log.debug(s"Put item outcome: ${outcome.getPutItemResult}")))
  }

  private def withTable(groupId: String, topic: Topic, offsets: Seq[OffsetTracking])(func: ((Table, Boolean)) => Future[Unit]): Future[Unit] = {

    val table = tableName(groupId, topic)
    Future {
      val tableObj = ddbClient.createTable(new CreateTableRequest()
        .withTableName(table)
        .withKeySchema(keySchema.asJava)
        .withAttributeDefinitions(attributeDefinitions.asJava)
        .withProvisionedThroughput(
          new ProvisionedThroughput()
            .withReadCapacityUnits(awsConfiguration.readCapacityUnits)
            .withWriteCapacityUnits(awsConfiguration.writeCapacityUnits)
        ))
      tableObj.waitForActive()
      (tableObj, true)
    }.recover {
      case tableExists: ResourceInUseException =>
        log.debug(s"Table $table already exists")
        (ddbClient.getTable(table), false)
    }.flatMap(func)
  }
}
