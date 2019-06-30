package xyz.callide.mesa.data

import org.scalatest.FlatSpec

class DataHeaderTests extends FlatSpec {

  behavior of "DataHeader"

  it should "construct a valid instance" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info)
    assert(header.fieldNames == List("A", "B"))
    assert(header.fieldName == Map(0 -> "A", 1 -> "B"))
    assert(header.fieldColumn == Map("A" -> 0, "B" -> 1))
    assert(header.fieldForm == Map("A" -> DataForm.Int, "B" -> DataForm.String))
  }

  it should "correctly append a field" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info).append("C", DataForm.Boolean)
    assert(header.fieldNames == List("A", "B", "C"))
    assert(header.fieldName == Map(0 -> "A", 1 -> "B", 2 -> "C"))
    assert(header.fieldColumn == Map("A" -> 0, "B" -> 1, "C" -> 2))
    assert(header.fieldForm == Map("A" -> DataForm.Int, "B" -> DataForm.String, "C" -> DataForm.Boolean))
  }

  it should "correctly prepend a field" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info).prepend("C", DataForm.Boolean)
    assert(header.fieldNames == List("C", "A", "B"))
    assert(header.fieldName == Map(1 -> "A", 2 -> "B", 0 -> "C"))
    assert(header.fieldColumn == Map("A" -> 1, "B" -> 2, "C" -> 0))
    assert(header.fieldForm == Map("A" -> DataForm.Int, "B" -> DataForm.String, "C" -> DataForm.Boolean))
  }

  it should "correctly remove a field" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info).append("C", DataForm.Boolean).remove("B")
    assert(header.fieldNames == List("A", "C"))
    assert(header.fieldName == Map(0 -> "A", 1 -> "C"))
    assert(header.fieldColumn == Map("A" -> 0, "C" -> 1))
    assert(header.fieldForm == Map("A" -> DataForm.Int, "C" -> DataForm.Boolean))
  }

  it should "correctly check if the header contains a field name" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info)
    assert(header.contains("A"))
    assert(!header.contains("C"))
  }
}
