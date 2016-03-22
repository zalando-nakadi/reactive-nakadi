package de.zalando.react.nakadi

import org.scalatest.{FlatSpec, Matchers}
import de.zalando.react.nakadi.NakadiMessages.{BeginOffset, Offset}


/**
  * Created by adrakeford on 22/03/2016.
  */
class NakadiMessagesTest extends FlatSpec with Matchers {

  "Offset object in NakadiMessages" should "be able to convert an offset string digit to a Offset (type Long) value" in {
    Offset("10").toLong should === (10L)
  }

  it should "be able to convert an offset string symbolic value to a Offset (type Long) value" in {
    BeginOffset.apply.toLong should === (0L)
    Offset("BEGIN").toLong should === (0L)
  }

  it should "be able to convert a single offset string digit to a Offset (type Long) value" in {
    Offset("3").toLong should === (3L)
  }

  it should "throw an error if there is an invalid offset value" in {
    intercept[IllegalArgumentException](Offset("something invalid").toLong)
  }

}
