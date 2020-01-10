package xyz.callide.mesa.csv

import java.time.{LocalDate, LocalDateTime}

import xyz.callide.mesa.data.{DataForm, DataSet}

trait CsvTest {

  def testCsvLoad(data: DataSet): Unit = {

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
    assert(data("age").extract[Int] == Vector(19, 29, 25))
    assert(data("dob").extract[LocalDate] == Vector(LocalDate.of(2000, 1, 1),
      LocalDate.of(1989, 3, 1),
      LocalDate.of(1994, 2, 1)))
    assert(data("score").extract[Double] == Vector(83.4, 65.1, 73.9))
    assert(data("pass").extract[Boolean] == Vector(true, false, true))
    assert(data("time").extract[LocalDateTime] == Vector(LocalDateTime.of(2018, 1, 1, 12, 0),
      LocalDateTime.of(2018, 1, 2, 14, 0),
      LocalDateTime.of(2018, 1, 3, 8, 0)))
  }
}
