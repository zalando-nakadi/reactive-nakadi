package de.zalando.react.nakadi

import com.typesafe.config.Config

import sys.process.Process


trait DockerProvider {
  def start: Int
  def stop: Int
}

class NakadiDockerProvider(config: Config) extends DockerProvider {

  override def start: Int = {
    Process(
      Seq("src/it/resources/nakadi.sh", "start"),
      new java.io.File("."),
      "DOCKER_IP" -> config.getString("docker.nakadi.host")) !
  }

  override def stop: Int = {
    Process(
      Seq("src/it/resources/nakadi.sh", "stop"),
      new java.io.File("."),
      "DOCKER_IP" -> config.getString("docker.nakadi.host")) !
  }
}
