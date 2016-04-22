package de.zalando.react.nakadi.client.models


trait EnumError {
  def message: String
}

sealed trait NamedEnum {
  def name: String
  override def toString(): String = name
}


object DataOpEnum {
  sealed trait DataOp extends NamedEnum

  case object C extends DataOp { override val name = "C" }
  case object U extends DataOp { override val name = "U" }
  case object D extends DataOp { override val name = "D" }
  case object S extends DataOp { override val name = "S" }
  case class UnknownValue(value: String) extends DataOp { override val name = s"UnknownValue($value)" }

  def apply(name: String): DataOp = name match {
    case C.name => C
    case U.name => U
    case D.name => D
    case S.name => S
    case _ => UnknownValue(name)
  }

  def contrapply(o: DataOp) = o.name
}

object EventTypeCategoryEnum {
  sealed trait Category extends NamedEnum

  case object Undefined extends Category { override val name = "undefined" }
  case object Data extends Category { override val name = "data" }
  case object Business extends Category { override val name = "business" }
  case class UnknownValue(value: String) extends Category { override val name = s"UnknownValue($value)" }

  def apply(name: String): Category = name match {
    case Undefined.name => Undefined
    case Data.name => Data
    case Business.name => Business
    case _ => UnknownValue(name)
  }

  def contrapply(o: Category) = o.name
}


