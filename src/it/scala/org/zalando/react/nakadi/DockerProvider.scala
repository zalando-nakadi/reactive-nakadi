package org.zalando.react.nakadi

import sys.process._

import scala.language.postfixOps


trait DockerProvider {
  def start: Int
  def stop: Int
}

class NakadiDockerProvider extends DockerProvider {

  override def start: Int = "src/it/resources/nakadi.sh start" !

  override def stop: Int = "src/it/resources/nakadi.sh stop" !

}
