package de.zalando.react.nakadi.commit.handlers

import de.zalando.react.nakadi.commit.Offsets


class MemoryCommitHandler extends BaseHandler {

  val store = scala.collection.concurrent.TrieMap.empty[String, Long]

  override def commitSync(offsets: Offsets): Unit = {
    offsets.foreach(v => store.put(v._1, v._2))
    println(s"Offsets: $store")
  }
}
