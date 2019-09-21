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

    val fields = Vector(new BooleanField(Array(true, false, false, true, true)),
                        new DoubleField(Array(1.234, 2.345, 3.456, 4.567, 5.678)),
                        new IntField(Array(2, 4, 6, 8, 10)),
                        new LongField(Array(1, 3, 5, 7, 9)),
                        new StringField(Array("abc", "def", "ghi", "jkl", "mno")),
                        new LocalDateField(dates),
                        new LocalDateTimeField(dateTimes))

    assert(data.fields == fields)
  }
}
