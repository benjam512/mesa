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

import java.io.{BufferedReader, File, FileReader}
import java.time.{LocalDate, LocalDateTime}

import org.scalatest.FlatSpec
import xyz.callide.mesa.csv.{CsvTest, CsvWriter}
import xyz.callide.mesa.ordering.OrderingDirection

class DataSetTests extends FlatSpec with DataSetUtil with CsvTest {

  behavior of "DataSet"

  it should "correctly read from CSV" in {

    val data = DataSet.fromCsvResource("data.csv")
    testCsvLoad(data)
  }

  it should "correctly read from CSV with missing values" in {

    val data = DataSet.fromCsvResource("data5.csv")
    assert(data.header.fieldNames == List("id", "name", "age", "dob", "score", "pass", "time"))
    assert(data.header.form("id") == DataForm.Long)
    assert(data.header.form("name") == DataForm.String)
    assert(data.header.form("age") == DataForm.Int)
    assert(data.header.form("dob") == DataForm.LocalDate)
    assert(data.header.form("score") == DataForm.Double)
    assert(data.header.form("pass") == DataForm.Boolean)
    assert(data.header.form("time") == DataForm.LocalDateTime)
    assert(data("id").extract[Long] == Vector(12345678901L, 23456789012L, 34567890123L))
    assert(data("name").extract[String] == Vector("bob", "joe", "sam"))
    assert(data("age").points == Vector(19, null, 25).map(v => DataPoint(Option(v))))
    assert(data("dob").extract[LocalDate] == Vector(LocalDate.of(2000, 1, 1),
      LocalDate.of(1989, 3, 1),
      LocalDate.of(1994, 2, 1)))
    assert(data("score").points == Vector(null, 65.1, 73.9).map(v => DataPoint(Option(v))))
    assert(data("pass").points == Vector(true, false, null).map(v => DataPoint(Option(v))))
    assert(data("time").points == Vector(null,
      LocalDateTime.of(2018, 1, 2, 14, 0),
      LocalDateTime.of(2018, 1, 3, 8, 0)).map(v => DataPoint(Option(v))))
  }

  it should "correctly write to a CSV" in {

    val fields = Vector(BooleanField(true, false, true), IntField(1, 3, 2), StringField("a", "b", "c"))
    val header = DataHeader(List(("BF", DataForm.Boolean), ("IF", DataForm.Int), ("SF", DataForm.String)))
    val data = DataSet(header, fields)
    val file = File.createTempFile("test-csv", ".csv")
    data.writeToCsv(file.getAbsolutePath) // no quotes
    val reader1 = new BufferedReader(new FileReader(file))
    assert(reader1.lines().toArray.map(_.toString).mkString("\n") == "BF,IF,SF\ntrue,1,a\nfalse,3,b\ntrue,2,c")
    reader1.close()
    data.writeToCsv(file.getAbsolutePath, quote = true) // no quotes
    val reader2 = new BufferedReader(new FileReader(file))
    assert(reader2.lines().toArray.map(_.toString).mkString("\n") ==
      "\"BF\",\"IF\",\"SF\"\n\"true\",\"1\",\"a\"\n\"false\",\"3\",\"b\"\n\"true\",\"2\",\"c\"")
    reader2.close()
  }

  it should "correctly count records" in {

    assert(data3.count == 4)
  }

  it should "correctly provide a data field" in {

    assert(data3("A").extract[Int] == Vector(123, 234, 345, 456))
  }

  it should "correctly provide a row" in {

    assert(data3.row(1) == DataRow(data3.header, Vector(234, true, 1.8)))
    assert(data3.row(3) == DataRow(data3.header, Vector(456, true, 2.4)))
  }

  it should "correctly provide a row iterator" in {

    assert(data3.rows.map(row => {
      !row[Boolean]("B")
    }).toVector == Vector(true, false, true, false))
  }

  it should "correctly append a field" in {

    val field = IntField(1, 2, 3, 4)
    val augmented = data3.append("D", field)
    assert(augmented.header.fieldNames == List("A", "B", "C", "D"))
    assert(augmented.header.form("A") == DataForm.Int)
    assert(augmented.header.form("B") == DataForm.Boolean)
    assert(augmented.header.form("C") == DataForm.Double)
    assert(augmented.header.form("D") == DataForm.Int)
    assert(augmented("D").extract[Int] == Vector(1, 2, 3, 4))
  }

  it should "correctly prepend a field" in {

    val field = IntField(1, 2, 3, 4)
    val augmented = data3.prepend("D", field)
    assert(augmented.header.fieldNames == List("D", "A", "B", "C"))
    assert(augmented.header.form("A") == DataForm.Int)
    assert(augmented.header.form("B") == DataForm.Boolean)
    assert(augmented.header.form("C") == DataForm.Double)
    assert(augmented.header.form("D") == DataForm.Int)
    assert(augmented("D").extract[Int] == Vector(1, 2, 3, 4))
  }

  it should "correctly drop a field" in {

    val diminished = data3.drop("B")
    assert(diminished.header.fieldNames == List("A", "C"))
    assert(diminished.row(0).elements == Vector(123, 0.5))
  }

  it should "correctly merge with another data set" in {

    val merged = data3.merge(data4)
    assert(merged.header.fieldNames == List("A", "B", "C", "D", "E"))
    assert(merged.header.form("A") == DataForm.Int)
    assert(merged.header.form("B") == DataForm.Boolean)
    assert(merged.header.form("C") == DataForm.Double)
    assert(merged.header.form("D") == DataForm.Int)
    assert(merged.header.form("E") == DataForm.String)
    assert(merged.row(0).elements == Vector(123, false, 0.5, 567, "abc"))
  }

  it should "correctly subset a data set" in {

    data3.subset(Seq(1, 3))("A").extract[Int] == Vector(234, 456)
  }

  it should "correctly select fields" in {

    val diminished = data3.select("A", "C")
    assert(diminished.row(0).elements == Vector(123, 0.5))
  }

  it should "correctly filter based on a field" in {

    val filtered = data3.filter("A")(point => point.as[Int] > 300)
    assert(filtered.count == 2)
    assert(filtered.row(0).elements == Vector(345, false, 3.5))
    assert(filtered.row(1).elements == Vector(456, true, 2.4))
  }

  it should "correctly filter based on a row" in {

    val filtered = data3.filter(row => row[Int]("A") > 200 && row[Boolean]("B"))
    assert(filtered.count == 2)
    assert(filtered.row(0).elements == Vector(234, true, 1.8))
    assert(filtered.row(1).elements == Vector(456, true, 2.4))
  }

  it should "correctly order according to a field" in {

    val ascending = data3.order("C")(point => point.as[Double])
    assert(ascending("A").extract[Int] == Vector(123, 234, 456, 345))
    assert(ascending("C").extract[Double] == Vector(0.5, 1.8, 2.4, 3.5))

    val descending = data3.order("C", OrderingDirection.Descending)(point => point.as[Double])
    assert(descending("A").extract[Int] == Vector(345, 456, 234, 123))
    assert(descending("C").extract[Double] == Vector(3.5, 2.4, 1.8, 0.5))
  }

  it should "correctly group by a field" in {

    val grouped = data3.group("B")(_.as[Boolean])
    assert(grouped.size == 2)
    val trueData = grouped(true)
    val falseData = grouped(false)
    assert(trueData.count == 2)
    assert(trueData.row(0).elements == Vector(234, true, 1.8))
    assert(trueData.row(1).elements == Vector(456, true, 2.4))
    assert(falseData.count == 2)
    assert(falseData.row(0).elements == Vector(123, false, 0.5))
    assert(falseData.row(1).elements == Vector(345, false, 3.5))
  }

  it should "correctly count a field condition" in {

    assert(data3.count("A")(_.as[Int] < 400) == 3)
    assert(data3.count("B")(_.as[Boolean]) == 2)
  }

  it should "correctly count a row condition" in {

    assert(data3.count(row => row[Boolean]("B") && row[Double]("C") > 2.0) == 1)
    assert(data3.count(row => row[Int]("A") > 200 && row[Double]("C") < 3.0) == 2)
  }
}
