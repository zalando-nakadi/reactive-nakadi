name := """reactive-nakadi"""
organization := "de.zalando"

version := "0.1.0-SNAPSHOT"
scalaVersion := "2.11.7"

val akkaVersion = "2.4.2"
val akkaExperimentalVersion = "2.0.3"

resolvers += "Maven Central Server" at "http://repo1.maven.org/maven2"
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.3",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.60",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-core" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaVersion,
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
)

mainClass in assembly := Some("de.zalando.react.nakadi.TestApp")
