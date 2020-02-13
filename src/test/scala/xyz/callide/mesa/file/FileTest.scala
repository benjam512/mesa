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

package xyz.callide.mesa.file

import java.time.{LocalDate, LocalDateTime}

import xyz.callide.mesa.data.{DataForm, DataSet}

trait FileTest {

  def testCompleteFile(data: DataSet): Unit = {

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

  def testPartialFile(data: DataSet): Unit = {

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
    assert(data("age").extract[Int] == Vector(19, 25))
    assert(data("dob").extract[LocalDate] == Vector(LocalDate.of(2000, 1, 1),
      LocalDate.of(1989, 3, 1),
      LocalDate.of(1994, 2, 1)))
    assert(data("score").extract[Double] == Vector(65.1, 73.9))
    assert(data("pass").extract[Boolean] == Vector(true, false))
    assert(data("time").extract[LocalDateTime] == Vector(
      LocalDateTime.of(2018, 1, 2, 14, 0),
      LocalDateTime.of(2018, 1, 3, 8, 0)))
  }
}
