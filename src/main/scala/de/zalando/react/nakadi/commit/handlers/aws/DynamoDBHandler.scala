package de.zalando.react.nakadi.commit.handlers.aws

import akka.actor.ActorSystem
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap

import de.zalando.react.nakadi.commit.handlers.BaseHandler
import de.zalando.react.nakadi.NakadiMessages.{Topic, Cursor}

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.document.{Item, Table}
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec

import scala.util.{Failure, Success}
import scala.concurrent.Future
import scala.collection.JavaConverters._


class DynamoDBHandler(system: ActorSystem, awsConfig: Option[AWSConfig] = None, clientProvider: Option[ClientProvider] = None) extends BaseHandler {

  import system.dispatcher

  private val log = system.log
  private lazy val awsConfiguration: AWSConfig = awsConfig.fold(AWSConfig())(cnf => cnf)
  private lazy val ddbClient = clientProvider.fold(ClientProvider(awsConfiguration.region))(provider => provider).client
  private val keySchema = Seq(new KeySchemaElement().withAttributeName("partitionId").withKeyType(KeyType.HASH))
  private val attributeDefinitions = Seq(new AttributeDefinition().withAttributeName("partitionId").withAttributeType(ScalarAttributeType.S))

  def tableName(groupId: String, topic: Topic) = s"reactive-nakadi-$topic-$groupId"

  override def commitSync(groupId: String, topic: Topic, cursors: Seq[Cursor]) = {
    update(groupId, topic).onComplete {
      case Failure(err) => log.error(err, "AWS Error")
      case Success(_) => println("done")
    }
  }

  def read(groupId: String, topic: Topic): Future[Item] = {

    val table = tableName(groupId, topic)
    Future {
      ddbClient.getTable(table).getItem("partitionId", "1")
    }
  }

  def create(groupId: String, topic: Topic): Future[Unit] = {

    withTable(groupId, topic) { table =>
      Future {
        table.putItem(new Item()
          .withPrimaryKey("partitionId", "1")
          .withString("checkpointId", "some_checkpoint_id")
          .withString("leaseHolder", "some_lease_holder")
          .withNumber("leaseCounter", 1)
          .withString("leaseTimestamp", "some_lease_id")
          .withString("leaseId", "some_lease_timestamp")
        )
      }.map { outcome =>
        log.debug(s"Put item outcome: ${outcome.getPutItemResult}")
      }
    }
  }

  def update(groupId: String, topic: Topic): Future[Unit] = {

    withTable(groupId, topic) { table =>
      Future {
        table.updateItem(new UpdateItemSpec()
          .withPrimaryKey("partitionId", "1")
          .withUpdateExpression(
            """
            |SET
            | checkpointId = :oval,
            | leaseHolder = :lhval,
            | leaseCounter = leaseCounter + :lcval,
            | leaseTimestamp = :ltsval,
            | leaseId = :lidval
            | """.stripMargin)
        .withValueMap(new ValueMap()
          .withString(":oval", "NEW_offset")
          .withString(":lhval", "NEW_leaseHolder")
          .withNumber(":lcval", 1)
          .withString(":ltsval", "NEW_leaseTimestamp")
          .withString(":lidval", "NEW_leaseId")
        ))
      }.map { outcome =>
        log.debug(s"Put item outcome: ${outcome.getUpdateItemResult}")
      }
    }
  }

  private def withTable(groupId: String, topic: Topic)(func: Table => Future[Unit]): Future[Unit] = {

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
    }.recover {
      case tableExists: ResourceInUseException =>
        log.debug(s"Table $table already exists")
        ddbClient.getTable(table)
    }.flatMap(func)
  }
}
