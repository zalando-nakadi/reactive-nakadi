name := """reactive-nakadi-core"""

val akkaVersion = "2.4.2"

resolvers += "Maven Central Server" at "http://repo1.maven.org/maven2"

val commonSettings = sonatypeSettings ++ Seq(
  organization := "org.zalando.reactivenakadi",
  version := "0.0.1-SNAPSHOT",
  startYear := Some(2016),
  scalaVersion := "2.11.7",
  test in assembly := {},
  licenses := Seq("Apache License 2.0" -> url("https://opensource.org/licenses/MIT")),
  homepage := Some(url("https://github.com/zalando/reactive-nakadi")),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    //"-Ywarn-value-discard",
    "-Xfuture"
  )
)

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.3",
  "com.typesafe.play" %% "play-json" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-core" % akkaVersion,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.60",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test, it",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test, it"
)

libraryDependencies ~= { _.map {
  case m if m.organization == "com.typesafe.play" =>
    m.exclude("commons-logging", "commons-logging")
  case m if m.organization == "com.typesafe.akka" =>
    m.exclude("commons-logging", "commons-logging")
  case m => m
}}

// causes merge problem when building fat JAR, but is not needed
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

parallelExecution in ThisBuild := false

val customItSettings = Defaults.itSettings ++ Seq(
  scalaSource := baseDirectory.value / "src" / "it",
  resourceDirectory := baseDirectory.value / "src" / "it" / "resources",
  fork in test := true,
  parallelExecution := false
)

val publishSettings = Seq(
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(inConfig(IntegrationTest)(customItSettings): _*)
  .settings(publishSettings)
