package de.zalando.react.nakadi

import com.typesafe.config.Config

import sys.process._


trait DockerProvider {
  def start: Int
  def stop: Int
}

class NakadiDockerProvider(config: Config) extends DockerProvider {

  override def start: Int = "src/it/resources/nakadi.sh start" !

  override def stop: Int = "src/it/resources/nakadi.sh stop" !

}
