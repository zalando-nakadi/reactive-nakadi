package de.zalando.react.nakadi.commit.handlers.dynamodb

import akka.actor.Actor

import de.zalando.react.nakadi.commit.handlers.BaseHandler
import de.zalando.react.nakadi.NakadiMessages.{Topic, Cursor}

/**
  * Created by adrakeford on 10/03/2016.
  */
class CommitHandler extends Actor with BaseHandler {

  override def receive: Receive = ???

  override def commitSync(groupId: String, topic: Topic, cursors: Seq[Cursor]): Unit = ???
}
