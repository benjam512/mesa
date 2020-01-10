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

package xyz.callide.mesa.csv

import java.io.{BufferedReader, File, FileReader}

import org.scalatest.FlatSpec
import xyz.callide.mesa.data.{BooleanField, DataForm, DataHeader, DataSet, IntField, StringField}

class CsvWriterTests extends FlatSpec {

  behavior of "CsvWriter"

  it should "write to a CSV file" in {

    val fields = Vector(BooleanField(true, false, true), IntField(1, 3, 2), StringField("a", "b", "c"))
    val header = DataHeader(List(("BF", DataForm.Boolean), ("IF", DataForm.Int), ("SF", DataForm.String)))
    val data = DataSet(header, fields)
    val file = File.createTempFile("test-csv", ".csv")
    CsvWriter(file.getAbsolutePath, ',', quote = false).write(data) // without quotes
    val reader1 = new BufferedReader(new FileReader(file))
    assert(reader1.lines().toArray.map(_.toString).mkString("\n") == "BF,IF,SF\ntrue,1,a\nfalse,3,b\ntrue,2,c")
    reader1.close()
    CsvWriter(file.getAbsolutePath, ',', quote = true).write(data) // with quotes
    val reader2 = new BufferedReader(new FileReader(file))
    assert(reader2.lines().toArray.map(_.toString).mkString("\n") ==
      "\"BF\",\"IF\",\"SF\"\n\"true\",\"1\",\"a\"\n\"false\",\"3\",\"b\"\n\"true\",\"2\",\"c\"")
    reader2.close()
  }
}
