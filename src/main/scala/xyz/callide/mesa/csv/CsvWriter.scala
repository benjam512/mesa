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

import java.io.PrintWriter

import xyz.callide.mesa.data.DataSet

/**
  * Writes data sets to a CSV file
  *
  * @param path the file path
  * @param delimiter the column delimiter
  * @param quote whether or not to place quotations around the elements
  */

case class CsvWriter(path: String, delimiter: Char, quote: Boolean) {

  /**
    * Writes the data set to a CSV file
    *
    * @param data the data set
    */

  def write(data: DataSet): Unit = {

    val writer = new PrintWriter(path)

    try {
      val delim = delimiter.toString
      val names = data.header.fieldNames
      writer.println((if (quote) names.map(name => "\"" + name + "\"") else names).mkString(delim))
      data.rows.foreach(row => {
        val line = if (quote) {
          names.map(name => "\"" + row[String](name) + "\"").mkString(delim)
        } else names.map(name => row[String](name)).mkString(delim)
        writer.println(line)
      })
    }
    finally {
      writer.close()
    }
  }
}
