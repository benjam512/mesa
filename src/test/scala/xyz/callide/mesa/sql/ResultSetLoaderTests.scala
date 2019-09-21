package xyz.callide.mesa.sql

import org.scalatest.FlatSpec
import xyz.callide.mesa.data.{BooleanField, DataForm, DataHeader, DataSet, DoubleField, IntField, LongField, StringField}

class ResultSetLoaderTests extends FlatSpec {

  behavior of "ResultSetLoader"

  it should "correctly load a data set from a result set" in {

    val data = DataSet.fromResultSet(new MockResultSet())
    assert(data.header == DataHeader(List(("booleans", DataForm.Boolean),
                                          ("doubles", DataForm.Double),
                                          ("ints", DataForm.Int),
                                          ("longs", DataForm.Long),
                                          ("strings", DataForm.String))))

    val fields = Vector(new BooleanField(Array(true, false, false, true, true)),
                        new DoubleField(Array(1.234, 2.345, 3.456, 4.567, 5.678)),
                        new IntField(Array(2, 4, 6, 8, 10)),
                        new LongField(Array(1, 3, 5, 7, 9)),
                        new StringField(Array("abc", "def", "ghi", "jkl", "mno")))

    assert(data.fields == fields)
  }
}
