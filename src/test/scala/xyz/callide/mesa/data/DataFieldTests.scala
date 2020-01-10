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

import java.time.{LocalDate, LocalDateTime}

import org.scalatest.FlatSpec

class DataFieldTests extends FlatSpec {

  behavior of "DataField"

  val field1 = new BooleanField(Array(Some(true), Some(false), Some(true)))
  val field2 = new BooleanField(Array(Some(true), None, Some(true)))

  it should "indicate if it has missing values" in {

    assert(!field1.hasMissingValues)
    assert(field2.hasMissingValues)
  }

  it should "provide a specified data point" in {

    assert(field1.point(1) == DataPoint(Some(false)))
    assert(field2.point(1) == DataPoint(None))
  }

  it should "provide a specified raw element" in {

    assert(field1.raw(1) == false)
    assert(field2.raw(1) == null)
  }

  it should "provide a vector of data points" in {

    assert(field1.points == Vector(DataPoint(Some(true)), DataPoint(Some(false)), DataPoint(Some(true))))
    assert(field2.points == Vector(DataPoint(Some(true)), DataPoint(None), DataPoint(Some(true))))
  }

  it should "extract elements without specifying a default value" in {

    assert(field1.extract[Boolean] == Vector(true, false, true))
    assert(field2.extract[Boolean] == Vector(true, true))
  }

  it should "extract elements with a specified default value" in {

    assert(field1.extract[Boolean](false) == Vector(true, false, true))
    assert(field2.extract[Boolean](false) == Vector(true, false, true))
  }

  it should "provide the correct length" in {

    assert(field1.length == 3)
    assert(field2.length == 3)
  }

  behavior of "BooleanField"

  it should "provide the correct data form" in {

    val field = BooleanField(true, false, true)
    assert(field.form == DataForm.Boolean)
  }

  it should "correctly subset" in {

    val field = BooleanField(true, false, true)
    assert(field.subset(List(0, 2)) == BooleanField(true, true))
  }

  it should "correctly test for equality" in {

    val field1 = BooleanField(true, false, true)
    val field2 = BooleanField(false, true, false)

    assert(field1 == BooleanField(true, false, true))
    assert(field1 != field2)
  }

  behavior of "LocalDateField"

  it should "provide the correct data form" in {

    val field = LocalDateField(LocalDate.of(2000, 1, 1), LocalDate.of(2000, 1, 2), LocalDate.of(2000, 1, 3))
    assert(field.form == DataForm.LocalDate)
  }

  it should "correctly subset" in {

    val field = LocalDateField(LocalDate.of(2000, 1, 1), LocalDate.of(2000, 1, 2), LocalDate.of(2000, 1, 3))
    assert(field.subset(List(0, 2)) == LocalDateField(LocalDate.of(2000, 1, 1), LocalDate.of(2000, 1, 3)))
  }

  it should "correctly test for equality" in {

    val field1 = LocalDateField(LocalDate.of(2000, 1, 1), LocalDate.of(2000, 1, 2), LocalDate.of(2000, 1, 3))
    val field2 = LocalDateField(LocalDate.of(2000, 1, 1), LocalDate.of(2000, 1, 3), LocalDate.of(2000, 1, 5))

    assert(field1 == LocalDateField(LocalDate.of(2000, 1, 1), LocalDate.of(2000, 1, 2), LocalDate.of(2000, 1, 3)))
    assert(field1 != field2)
  }

  behavior of "LocalDateTimeField"

  it should "provide the correct data form" in {

    val field = LocalDateTimeField(LocalDateTime.of(2000, 1, 1, 12, 0), LocalDateTime.of(2000, 1, 2, 12, 0),
                                         LocalDateTime.of(2000, 1, 3, 12, 0))
    assert(field.form == DataForm.LocalDateTime)
  }

  it should "correctly subset" in {

    val field = LocalDateTimeField(LocalDateTime.of(2000, 1, 1, 12, 0), LocalDateTime.of(2000, 1, 2, 12, 0),
      LocalDateTime.of(2000, 1, 3, 12, 0))
    assert(field.subset(List(0, 2)) == LocalDateTimeField(LocalDateTime.of(2000, 1, 1, 12, 0), LocalDateTime.of(2000, 1, 3, 12, 0)))
  }

  it should "correctly test for equality" in {

    val field1 = LocalDateTimeField(LocalDateTime.of(2000, 1, 1, 12, 0), LocalDateTime.of(2000, 1, 2, 12, 0),
      LocalDateTime.of(2000, 1, 3, 12, 0))
    val field2 = LocalDateTimeField(LocalDateTime.of(2000, 1, 1, 12, 0), LocalDateTime.of(2000, 1, 3, 12, 0),
      LocalDateTime.of(2000, 1, 5, 12, 0))

    assert(field1 == LocalDateTimeField(LocalDateTime.of(2000, 1, 1, 12, 0), LocalDateTime.of(2000, 1, 2, 12, 0),
      LocalDateTime.of(2000, 1, 3, 12, 0)))
    assert(field1 != field2)
  }

  behavior of "DoubleField"

  it should "provide the correct data form" in {

    val field = DoubleField(1.5, 2.5, 3.5)
    assert(field.form == DataForm.Double)
  }

  it should "correctly subset" in {

    val field = DoubleField(1.5, 2.5, 3.5)
    assert(field.subset(List(0, 2)) == DoubleField(1.5, 3.5))
  }

  it should "correctly test for equality" in {

    val field1 = DoubleField(1.5, 2.5, 3.5)
    val field2 = DoubleField(1.5, 7.5, 3.5)

    assert(field1 == DoubleField(1.5, 2.5, 3.5))
    assert(field1 != field2)
  }

  behavior of "IntField"

  it should "provide the correct data form" in {

    val field = IntField(1, 2, 3)
    assert(field.form == DataForm.Int)
  }

  it should "correctly subset" in {

    val field = IntField(1, 2, 3)
    assert(field.subset(List(0, 2)) == IntField(1, 3))
  }

  it should "correctly test for equality" in {

    val field1 = IntField(1, 2, 3)
    val field2 = IntField(1, 3, 5)

    assert(field1 == IntField(1, 2, 3))
    assert(field1 != field2)
  }

  behavior of "LongField"

  it should "provide the correct data form" in {

    val field = LongField(1, 2, 3)
    assert(field.form == DataForm.Long)
  }

  it should "correctly subset" in {

    val field = LongField(1, 2, 3)
    assert(field.subset(List(0, 2)) == LongField(1, 3))
  }

  it should "correctly test for equality" in {

    val field1 = LongField(1, 2, 3)
    val field2 = LongField(1, 3, 5)

    assert(field1 == LongField(1, 2, 3))
    assert(field1 != field2)
  }

  behavior of "StringField"

  it should "provide the correct data form" in {

    val field = StringField("a", "b", "c")
    assert(field.form == DataForm.String)
  }

  it should "correctly subset" in {

    val field = StringField("a", "b", "c")
    assert(field.subset(List(0, 2)) == StringField("a", "c"))
  }

  it should "correctly test for equality" in {

    val field1 = StringField("a", "b", "c")
    val field2 = StringField("a", "b", "d")

    assert(field1 == StringField("a", "b", "c"))
    assert(field1 != field2)
  }
}
