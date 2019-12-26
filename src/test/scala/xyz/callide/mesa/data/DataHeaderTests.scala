/*
   Copyright 2018 Callide Technologies LLC

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  */

package xyz.callide.mesa.data

import org.scalatest.FlatSpec

class DataHeaderTests extends FlatSpec {

  behavior of "DataHeader"

  it should "construct a valid instance" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info)
    assert(header.fieldNames == List("A", "B"))
    assert(header.name(0) == "A")
    assert(header.name(1) == "B")
    assert(header.column("A") == 0)
    assert(header.column("B") == 1)
    assert(header.form("A") == DataForm.Int)
    assert(header.form("B") == DataForm.String)
  }

  it should "correctly append a field" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info).append("C", DataForm.Boolean)
    assert(header.fieldNames == List("A", "B", "C"))
    assert(header.name(0) == "A")
    assert(header.name(1) == "B")
    assert(header.name(2) == "C")
    assert(header.column("A") == 0)
    assert(header.column("B") == 1)
    assert(header.column("C") == 2)
    assert(header.form("A") == DataForm.Int)
    assert(header.form("B") == DataForm.String)
    assert(header.form("C") == DataForm.Boolean)
  }

  it should "correctly prepend a field" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info).prepend("C", DataForm.Boolean)
    assert(header.fieldNames == List("C", "A", "B"))
    assert(header.name(0) == "C")
    assert(header.name(1) == "A")
    assert(header.name(2) == "B")
    assert(header.column("A") == 1)
    assert(header.column("B") == 2)
    assert(header.column("C") == 0)
    assert(header.form("A") == DataForm.Int)
    assert(header.form("B") == DataForm.String)
    assert(header.form("C") == DataForm.Boolean)
  }

  it should "correctly remove a field" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info).append("C", DataForm.Boolean).remove("B")
    assert(header.fieldNames == List("A", "C"))
    assert(header.name(0) == "A")
    assert(header.name(1) == "C")
    assert(header.column("A") == 0)
    assert(header.column("C") == 1)
    assert(header.form("A") == DataForm.Int)
    assert(header.form("C") == DataForm.Boolean)
  }

  it should "correctly check if the header contains a field name" in {

    val info = List(("A", DataForm.Int), ("B", DataForm.String))
    val header = DataHeader(info)
    assert(header.contains("A"))
    assert(!header.contains("C"))
  }
}
