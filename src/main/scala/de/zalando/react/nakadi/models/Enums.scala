package de.zalando.react.nakadi.models

import scalaz._
import Scalaz._

trait EnumError {
  def message: String
}
case class BadValue(message: String) extends EnumError

sealed trait NamedEnum {
  def name: String
  override def toString(): String = name
}


object DataChangeEventQualifierDataOpEnum {
  sealed trait DataOp extends NamedEnum

  case object C extends DataOp { override val name = "C" }
  case object U extends DataOp { override val name = "U" }
  case object D extends DataOp { override val name = "D" }
  case object S extends DataOp { override val name = "S" }
  case class UnknownValue(value: String) extends DataOp { override val name = s"UnknownValue($value)" }

  def apply(name: String): EnumError \/ DataOp = name match {
    case C.name => C.right
    case U.name => U.right
    case D.name => D.right
    case S.name => S.right
    case _ => UnknownValue(name).right
  }
}


object EventTypeCategoryEnum {
  sealed trait Category extends NamedEnum

  case object Undefined extends Category { override val name = "undefined" }
  case object Data extends Category { override val name = "data" }
  case object Business extends Category { override val name = "business" }
  case class UnknownValue(value: String) extends Category { override val name = s"UnknownValue($value)" }

  def apply(name: String): EnumError \/ Category = name match {
    case Undefined.name => Undefined.right
    case Data.name => Data.right
    case Business.name => Business.right
    case _ => UnknownValue(name).right
  }
}


