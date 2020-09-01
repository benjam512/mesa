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

import java.io.{File, InputStream}

import com.univocity.parsers.csv.{CsvParserSettings, UnescapedQuoteHandling}
import xyz.callide.mesa.data.conversion.ConversionSet
import xyz.callide.mesa.data.DataSet


/**
  * Provides functionality for loading CSV data
  */

object CsvLoader extends FileLoader {

  /**
    * Reads from the provided input stream
    *
    * @param stream the input stream
    * @param delimiter column separator
    * @param header denotes whether or not the CSV file contains a header row
    * @param set conversion set
    * @return data set
    */

  def readFromStream(stream: InputStream,
                     delimiter: Char = ',',
                     header: Boolean = true)(implicit set: ConversionSet): DataSet = {

    val settings = new CsvParserSettings()
    settings.getFormat.setDelimiter(delimiter)
    settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_CLOSING_QUOTE)
    settings.setMaxCharsPerColumn(65536)

    val parser = new com.univocity.parsers.csv.CsvParser(settings)
    read(new CsvRowIterator(parser.iterate(stream).iterator()), header)
  }

  /**
    * Reads from the provided file path
    *
    * @param path the file path
    * @param delimiter column separator
    * @param header denotes whether or not the CSV file contains a header row
    * @param set conversion set
    * @return data set
    */

  def readFromPath(path: String,
                   delimiter: Char = ',',
                   header: Boolean = true)(implicit set: ConversionSet): DataSet = {

    readFromFile(new File(path), delimiter, header)
  }

  /**
    * Reads from the provided file
    *
    * @param file the input file
    * @param delimiter column separator
    * @param header denotes whether or not the CSV file contains a header row
    * @param set conversion set
    * @return data set
    */

  def readFromFile(file: File,
                   delimiter: Char,
                   header: Boolean)(implicit set: ConversionSet): DataSet = {

    val settings = new CsvParserSettings()
    settings.getFormat.setDelimiter(delimiter)
    settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_CLOSING_QUOTE)
    settings.setMaxCharsPerColumn(65536)

    val parser = new com.univocity.parsers.csv.CsvParser(settings)
    read(new CsvRowIterator(parser.iterate(file).iterator()), header)
  }
}
