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

package xyz.callide.mesa.sql

import java.time.{LocalDate, LocalDateTime}

import org.scalatest.FlatSpec
import xyz.callide.mesa.data.{BooleanField, DataForm, DataHeader, DataSet, DoubleField, IntField, LocalDateField, LocalDateTimeField, LongField, StringField}

class ResultSetLoaderTests extends FlatSpec {

  behavior of "ResultSetLoader"

  it should "correctly load a data set from a result set" in {

    val data = DataSet.fromResultSet(new MockResultSet())
    assert(data.header == DataHeader(List(("booleans", DataForm.Boolean),
                                          ("doubles", DataForm.Double),
                                          ("ints", DataForm.Int),
                                          ("longs", DataForm.Long),
                                          ("strings", DataForm.String),
                                          ("dates", DataForm.LocalDate),
                                          ("date-times", DataForm.LocalDateTime))))

    val dates = Array(LocalDate.of(2000, 1, 1), LocalDate.of(2000, 1, 2), LocalDate.of(2000, 1, 3),
                      LocalDate.of(2000, 1, 4), LocalDate.of(2000, 1, 5))

    val dateTimes = Array(LocalDateTime.of(1999, 12, 31, 7, 0), LocalDateTime.of(1999, 12, 30, 7, 0),
                          LocalDateTime.of(1999, 12, 29, 7, 0), LocalDateTime.of(1999, 12, 28, 7, 0),
                          LocalDateTime.of(1999, 12, 27, 7, 0))

    val fields = Vector(BooleanField(Array(true, false, false, true, true):_*),
                        DoubleField(Array(1.234, 2.345, 3.456, 4.567, 5.678):_*),
                        IntField(Array(2, 4, 6, 8, 10):_*),
                        LongField(Array(1L, 3, 5, 7, 9):_*),
                        StringField(Array("abc", "def", "ghi", "jkl", "mno"):_*),
                        LocalDateField(dates:_*),
                        LocalDateTimeField(dateTimes:_*))

    assert(data.fields == fields)
  }
}
