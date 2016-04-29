package de.zalando.react.nakadi.commit.handlers.aws

import akka.actor.ActorSystem
import de.zalando.react.nakadi.commit.OffsetTracking

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

  def tableName(groupId: String, topic: String) = s"reactive-nakadi-$topic-$groupId"

  override def get(groupId: String, topic: String, partitionId: String): Future[Option[OffsetTracking]] = {

    withTable(groupId, topic) { table =>
      Future {
        Option(table.getItem(PartitionIdKey, partitionId)).map(toOffsetTracking)
      }
    }
  }

  override def put(groupId: String, topic: String, offset: OffsetTracking): Future[OffsetTracking] = {

    withTable(groupId, topic) { table =>
      Future {
        Option(table.getItem(PartitionIdKey, offset.partitionId))
          .fold(handlePutItem _)(_ => handleUpdateItem _)(table, groupId, topic, offset)
      }
    }
  }

  private def handleUpdateItem(table: Table, groupId: String, topic: String, offsetTracking: OffsetTracking): OffsetTracking = {
    val valueMap = new ValueMap()
      .withString(":cidval", offsetTracking.checkpointId)
      .withString(":lhval", offsetTracking.leaseHolder)
      .withString(":ltsval", offsetTracking.leaseTimestamp.toDateTime.toString)

    var leaseIdKey = ""
    offsetTracking.leaseId.foreach { leaseId =>
      valueMap.withString(":lidval", leaseId)
      leaseIdKey = s", $LeaseIdKey = :lidval"
    }

    // Allow for overwriting of lease counter
    val countExpression = offsetTracking.leaseCounter.fold {
      valueMap.withNumber(":lcval", 1)
      "leaseCounter + :lcval"
    } { count =>
      valueMap.withNumber(":lcval", count)
      ":lcval"
    }

    val expression = {
      s"""
         |SET
         | $CheckpointIdKey = :cidval,
         | $LeaseHolderKey = :lhval,
         | $LeaseCounterKey = $countExpression,
         | $LeaseTimestampKey = :ltsval $leaseIdKey
         | """.stripMargin
    }

    table.updateItem(new UpdateItemSpec()
      .withPrimaryKey(PartitionIdKey, offsetTracking.partitionId)
      .withUpdateExpression(expression)
      .withValueMap(valueMap))
    toOffsetTracking(table.getItem(PartitionIdKey, offsetTracking.partitionId))
  }

  private def handlePutItem(table: Table, groupId: String, topic: String, offsetTracking: OffsetTracking): OffsetTracking = {

    val item = new Item()
      .withPrimaryKey(PartitionIdKey, offsetTracking.partitionId)
      .withString(CheckpointIdKey, offsetTracking.checkpointId)
      .withString(LeaseHolderKey, offsetTracking.leaseHolder)
      .withNumber(LeaseCounterKey, 1)
      .withString(LeaseTimestampKey, offsetTracking.leaseTimestamp.toDateTime.toString)
    offsetTracking.leaseId.map(item.withString(LeaseIdKey, _))
    table.putItem(item)
    toOffsetTracking(item)
  }

  private def withTable[T](groupId: String, topic: String)(func: Table => Future[T]): Future[T] = {

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
      tableObj
    }.recoverWith {
      case tableExists: ResourceInUseException =>
        Future(ddbClient.getTable(table))
    }.flatMap(func)
  }

  private def toOffsetTracking(item: Item): OffsetTracking = {
    OffsetTracking(
      partitionId = item.getString(PartitionIdKey),
      checkpointId = item.getString(CheckpointIdKey),
      leaseHolder = item.getString(LeaseHolderKey),
      leaseCounter = Option(item.getLong(LeaseCounterKey)),
      leaseTimestamp = new DateTime(item.getString(LeaseTimestampKey), DateTimeZone.UTC),
      leaseId = Option(item.getString(LeaseIdKey))
    )
  }
}
