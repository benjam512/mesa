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

import org.scalatest.FlatSpec
import xyz.callide.mesa.data.conversion.ConversionSet

class ExcelLoaderTests extends FlatSpec with FileTest {

  behavior of "ExcelLoader"

  it should "read from file" in {

    testCompleteFile(ExcelLoader.readFromFile(getClass.getClassLoader.getResource("data.xlsx").getFile)(ConversionSet()))
    testPartialFile(ExcelLoader.readFromFile(getClass.getClassLoader.getResource("data5.xlsx").getFile)(ConversionSet()))
  }
}
