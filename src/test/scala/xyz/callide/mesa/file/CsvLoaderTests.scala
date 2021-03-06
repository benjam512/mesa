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

import java.io.File

import org.scalatest.FlatSpec
import xyz.callide.mesa.data.conversion.ConversionSet

class CsvLoaderTests extends FlatSpec with FileTest {

  behavior of "CsvLoader"

  it should "read from file" in {

    testCompleteFile(CsvLoader.readFromPath(getClass.getClassLoader.getResource("data.csv").getFile)(ConversionSet()))
    testPartialFile(CsvLoader.readFromPath(getClass.getClassLoader.getResource("data5.csv").getFile)(ConversionSet()))
    testCompleteFile(CsvLoader.readFromFile(new File(getClass.getClassLoader.getResource("data.csv").toURI), ',', true, None)(ConversionSet()))
    testPartialFile(CsvLoader.readFromFile(new File(getClass.getClassLoader.getResource("data5.csv").toURI), ',', true, None)(ConversionSet()))
  }

  it should "read from stream" in {

    testCompleteFile(CsvLoader.readFromStream(getClass.getClassLoader.getResourceAsStream("data.csv"))(ConversionSet()))
    testPartialFile(CsvLoader.readFromStream(getClass.getClassLoader.getResourceAsStream("data5.csv"))(ConversionSet()))
  }

  it should "read from file with comment markers" in {

    val data1 = CsvLoader.readFromPath(getClass.getClassLoader.getResource("data6.csv").getFile)(ConversionSet())
    assert(data1.count == 2)
    assert(data1.row(1)[String]("Col-A") == "#!*")

    val data2 = CsvLoader.readFromPath(getClass.getClassLoader.getResource("data6.csv").getFile, comment = Some('#'))(ConversionSet())
    assert(data2.count == 1)
    assert(data2.row(0)[String]("Col-A") == "ABC")
  }
}
