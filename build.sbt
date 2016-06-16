name := """reactive-nakadi-core"""

val akkaVersion = "2.4.2"

resolvers += "Maven Central Server" at "http://repo1.maven.org/maven2"

val commonSettings = sonatypeSettings ++ Seq(
  organization := "org.zalando.reactivenakadi",
  startYear := Some(2016),
  scalaVersion := "2.11.7",
  test in assembly := {},
  licenses := Seq("MIT license" -> url("https://opensource.org/licenses/MIT")),
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
  "commons-logging" % "commons-logging" % "1.1.1",
  "com.typesafe.play" %% "play-json" % "2.5.4",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-core" % akkaVersion,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.60",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test, it",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test, it"
)

// Exclude common logging in all dependancies, and just have it as a top level dependency in this project
libraryDependencies ~= { _ map(_.exclude("commons-logging", "commons-logging")) }

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

// Setup Publishing settings
credentials ++= (for {
  username <- Option(System.getenv().get("SONATYPE_USERNAME"))
  password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq

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

// Setup Git Versioning
lazy val gitVersioningSettings = Seq(
  showCurrentGitBranch,
  git.baseVersion := "v0.0.01",
  git.useGitDescribe := true,
  git.gitTagToVersionNumber := { version: String =>
    val VersionRegex = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r
    version match {
      case VersionRegex(v,"SNAPSHOT") => Some(s"$v-SNAPSHOT")
      case VersionRegex(v,"") => Some(v)
      case VersionRegex(v,s) => Some(s"$v-$s-SNAPSHOT")
      case _ => None
    }
  }
)

lazy val root = (project in file("."))
  .enablePlugins(GitVersioning)
  .configs(IntegrationTest)
  .settings(gitVersioningSettings: _*)
  .settings(commonSettings)
  .settings(inConfig(IntegrationTest)(customItSettings): _*)
  .settings(publishSettings)
