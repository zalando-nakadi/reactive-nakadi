package de.zalando.react.nakadi.commit

import org.scalatest.{Matchers, FlatSpec}


class OffsetMapSpec extends FlatSpec with Matchers {

  "OffsetMap" should "return an offset given a partitoin" in {
    val offset = OffsetMap(Map("1" -> 10, "2" -> 0))
    offset.lastOffset("1") should === (10)
  }

  it should "return -1 if partition not found" in {
    val offset = OffsetMap(Map("1" -> 10, "2" -> 0))
    offset.lastOffset("3") should === (-1L)
  }

  it should "return a difference of two offsets" in {
    var offset1 = OffsetMap(Map("1" -> 10))
    var offset2 = OffsetMap(Map("2" -> 12))
    offset1 diff offset2 should === (OffsetMap(Map("1" -> 10)))

    offset1 = OffsetMap(Map("1" -> 10, "2" -> 2))
    offset2 = OffsetMap(Map("2" -> 12))
    offset1 diff offset2 should === (OffsetMap(Map("1" -> 10, "2" -> 2)))

    offset1 = OffsetMap(Map("2" -> 12))
    offset2 = OffsetMap(Map("1" -> 10, "2" -> 2))
    offset1 diff offset2 should === (OffsetMap(Map("2" -> 12)))
  }

  it should "return empty offset map if both empty" in {
    OffsetMap() diff OffsetMap() should === (OffsetMap())
  }

  it should "be able to add a new offset and return a new instance" in {
    val offset = OffsetMap(Map("1" -> 10))
    offset.plusOffset("2", 20) should === (OffsetMap(Map("1" -> 10, "2" -> 20)))
    offset.plusOffset("2", 30) should === (OffsetMap(Map("1" -> 10, "2" -> 30)))
  }

  it should "be able to update an existing offset" in {
    val offset = OffsetMap(Map("1" -> 10))
    offset.updateWithOffset("1", 20)
    offset should === (OffsetMap(Map("1" -> 20)))
  }

  it should "correctly identify nonEmpty" in {
    OffsetMap(Map("1" -> 10)).nonEmpty should === (true)
  }

  it should "be able to convert toCommitRequestInfo" in {
    import de.zalando.react.nakadi.NakadiMessages.Cursor

    val offset = OffsetMap(Map("1" -> 10, "2" -> 20))
    val expected = Seq(Cursor(partition = "1", offset = "10"), Cursor(partition = "2", offset = "20"))
    offset.toCommitRequestInfo should === (expected)
  }

  "OffsetMap object" should "be able to convert an offset string digit to a Offset (type Long) value" in {
    OffsetMap.offsetFromString("10") should === (10L)
  }

  it should "be able to convert an offset string symbolic value to a Offset (type Long) value" in {
    OffsetMap.offsetFromString("BEGIN") should === (0L)
  }

  it should "be able to convert a single offset string digit to a Offset (type Long) value" in {
    OffsetMap.offsetFromString("3") should === (3L)
  }

  it should "throw an error if there is an invalid offset value" in {
    intercept[IllegalArgumentException](OffsetMap.offsetFromString("something invalid"))
  }

  it should "create a new instance with empty map" in {
    OffsetMap().map should === (Map.empty)
  }

}
