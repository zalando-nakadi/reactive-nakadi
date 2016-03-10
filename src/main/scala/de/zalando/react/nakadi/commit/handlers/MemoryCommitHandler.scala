package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.NakadiMessages.{Topic, Cursor}


class MemoryCommitHandler extends BaseHandler {

  val store = scala.collection.concurrent.TrieMap.empty[String, String]

  override def commitSync(groupId: String, topic: Topic, cursors: Seq[Cursor]): Unit = {
    cursors.foreach(cursor => store.put(cursor.partition, cursor.offset))
    println(s"committed offsets: $store")
  }
}
