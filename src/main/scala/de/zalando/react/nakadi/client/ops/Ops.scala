package de.zalando.react.nakadi.client.ops

case class Id[V, T](value: V) extends AnyVal {
  override def toString = value.toString
  def convert[O](f: V => O): O = f(value)
  def as[T2]: Id[V, T2] = convert(Id[V, T2])
}


object Id {
  import play.api.libs.json._

  implicit def idFormat[V, T](implicit vf: Format[V]) = Format[Id[V, T]](Reads(_.validate[V].map(Id[V, T])), Writes(v => Json.toJson(v.value)))

  implicit class IdOps[V](val self: V) extends AnyVal {
    def id[T] = Id[V, T](self)
  }

}

object IdBindables {
  import play.api.mvc.{PathBindable, QueryStringBindable}

  implicit def idPathBindable[V, T](implicit binder: PathBindable[V]) = new PathBindable[Id[V, T]] {
    override def bind(key: String, value: String) = binder.bind(key, value).right.map(Id[V, T](_))
    override def unbind(key: String, value: Id[V, T]) = binder.unbind(key, value.value)
  }

  implicit def idQueryStringBindable[V, T](implicit binder: QueryStringBindable[V]) = new QueryStringBindable[Id[V, T]] {
    override def bind(key: String, params: Map[String, Seq[String]]): Option[Either[String, Id[V, T]]] = {
      binder.bind(key, params).map(_.right.map(Id[V, T]))
    }
    override def unbind(key: String, value: Id[V, T]): String = binder.unbind(key, value.value)
  }
}