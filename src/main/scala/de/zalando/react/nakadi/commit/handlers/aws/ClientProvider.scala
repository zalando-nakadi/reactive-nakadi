package de.zalando.react.nakadi.commit.handlers.aws

import com.amazonaws.regions.Regions
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient


case class ClientProvider(client: DynamoDB)

object ClientProvider {

  def apply(region: String, credentialsProvider: Option[ProfileCredentialsProvider] = None): ClientProvider = {
    val client = new AmazonDynamoDBClient(credentialsProvider.fold(new ProfileCredentialsProvider())(cred => cred))
    client.withRegion(Regions.fromName(region))
    new ClientProvider(client = new DynamoDB(client))
  }
}
