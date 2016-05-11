package org.zalando.react.nakadi.utils


trait HashService {
  def generate[T](value: T): String
}

object HashService extends HashService {

  private def md5(s: String) = {
    import java.security.MessageDigest

    val m = MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(16)
  }

  def generate[T](value: T): String = md5(value.toString)
}
