package de.zalando.react.nakadi.leases.impl

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.document.DynamoDB


case class AWSProvider(client: AmazonDynamoDB,
                       readCapacityUnits: Long,
                       writeCapacityUnits: Long,
                       consistentReads: Boolean)

object AWSProvider {

  def apply(region: String, credentialsProvider: Option[ProfileCredentialsProvider] = None): AWSProvider = {
//    val client = new AmazonDynamoDBClient(credentialsProvider.fold(new ProfileCredentialsProvider())(cred => cred))
//    client.withRegion(Regions.fromName(region))
//    new AWSClientProvider(client = new DynamoDB(client))
    ???
  }
}
