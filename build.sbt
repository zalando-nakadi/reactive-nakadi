name := """reactive-nakadi"""
organization := "de.zalando"

version := "0.1.0-SNAPSHOT"
scalaVersion := "2.11.7"

val akkaVersion = "2.4.2"

parallelExecution in ThisBuild := false

resolvers += "Maven Central Server" at "http://repo1.maven.org/maven2"

val customItSettings = Defaults.itSettings ++ Seq(
  scalaSource := baseDirectory.value / "src" / "it",
  resourceDirectory := baseDirectory.value / "src" / "it" / "resources",
  fork in test := true,
  parallelExecution := false
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(customItSettings): _*)


libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.3",
  "org.asynchttpclient" % "async-http-client" % "2.0.0-RC9",
  "com.typesafe.play" %% "play-json" % "2.4.3",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.60",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-core" % akkaVersion,
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test, it",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test, it"
)

// commons-logging in play-ws_2.11 and aws-java-sdk-dynamodb is conflicting with slf4j-api
//libraryDependencies ~= { _ map(_.exclude("commons-logging", "commons-logging"))}

// causes merge problem when building fat JAR, but is not needed
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

mainClass in assembly := Some("de.zalando.react.nakadi.TestApp")
