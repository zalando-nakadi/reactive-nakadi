package de.zalando.react.nakadi.client.models


object OffsetSymbolicValue {

  sealed trait OffsetSymbolicValueEnum {
    def name: String
    override def toString: String = name.toUpperCase
  }

  case object Begin extends OffsetSymbolicValueEnum { override val name = "BEGIN" }

  def apply(name: String): OffsetSymbolicValueEnum = name.toUpperCase match {
    case Begin.name => Begin
    case _ => sys.error(s"Unknown value '$name' for 'attribute-type' enum")
  }
}

