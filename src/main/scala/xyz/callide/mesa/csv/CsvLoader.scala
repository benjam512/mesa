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

import java.io.{File, InputStream}

import com.univocity.parsers.common.{ParsingContext, ResultIterator}
import com.univocity.parsers.csv.{CsvParserSettings, UnescapedQuoteHandling}
import xyz.callide.mesa.data.builder.DataFieldBuilder
import xyz.callide.mesa.data.builder.DataFieldBuilder.{BooleanFieldBuilder, DoubleFieldBuilder, IntFieldBuilder, LocalDateFieldBuilder, LocalDateTimeFieldBuilder, LongFieldBuilder, StringFieldBuilder}
import xyz.callide.mesa.data.conversion.ConversionSet
import xyz.callide.mesa.data.{DataForm, DataHeader, DataSet}
import xyz.callide.mesa.data.DataForm.FormInference

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Try}

/**
  * Provides functionality for loading CSV data
  */

object CsvLoader {

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
    read(parser.iterate(stream).iterator(), header)
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

  def readFromFile(path: String,
                   delimiter: Char = ',',
                   header: Boolean = true)(implicit set: ConversionSet): DataSet = {

    val settings = new CsvParserSettings()
    settings.getFormat.setDelimiter(delimiter)
    settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_CLOSING_QUOTE)
    settings.setMaxCharsPerColumn(65536)

    val parser = new com.univocity.parsers.csv.CsvParser(settings)
    read(parser.iterate(new File(path)).iterator(), header)
  }

  /**
    * Reads from the provided iterator
    *
    * @param iterator result iterator
    * @param set conversion set
    * @return data set
    */

  private def read(iterator: ResultIterator[Array[String], ParsingContext],
                   header: Boolean)(implicit set: ConversionSet): DataSet = {

    val (names, firstLine) = if (iterator.hasNext) {
      val head = if (header) Some(iterator.next) else None
      val line = if (iterator.hasNext) iterator.next else throw new IllegalArgumentException("No data")
      (head.getOrElse(Array.range(0, line.length).map(i => "V" + (i + 1).toString)), line)
    } else throw new IllegalArgumentException("No data")

    val forms = inferForms(firstLine)
    val builders = initBuilders(forms, firstLine)

    while (iterator.hasNext) {
      val line = iterator.next()
      var j = 0
      while (j < line.length) {
        Try(builders(j).append(line(j))) match {
          case Failure(_) =>
            forms(j).infer(line(j))
            builders(j) = switch(builders(j), forms(j))
            builders(j).append(line(j))
          case _ => Unit
        }
        j += 1
      }
    }

    DataSet(DataHeader(names.zip(forms.map(_.getForm.get))), builders.map(_.toField).toVector)
  }

  /**
    * Infers the initial data forms from the first line of data
    *
    * @param line first line of data
    * @param set conversion set
    * @return initial form inference estimates
    */

  private def inferForms(line: Array[String])(implicit set: ConversionSet): Array[FormInference] = {

    line.map(elem => {
      val fi = new FormInference(set)
      fi.infer(elem)
      fi
    })
  }

  /**
    * Initializes the field builders
    *
    * @param forms initial form inferences
    * @param line first line of data
    * @param set conversion set
    * @return data set
    */

  private def initBuilders(forms: Array[FormInference], line: Array[String])
                          (implicit set: ConversionSet): Array[DataFieldBuilder] = {

    forms.zipWithIndex.map({case (inf, ind) =>
      val elem = line(ind) match {
        case null => None
        case "" => None
        case v if v.forall(c => c == ' ') => None
        case v => Some(v)
      }
      inf.getForm match {
        case Some(DataForm.Boolean) =>
          new BooleanFieldBuilder(ArrayBuffer(elem.map(set.booleanConverter.convert)))
        case Some(DataForm.LocalDate) =>
          new LocalDateFieldBuilder(ArrayBuffer(elem.map(set.localDateConverter.convert)))
        case Some(DataForm.LocalDateTime) =>
          new LocalDateTimeFieldBuilder(ArrayBuffer(elem.map(set.localDateTimeConverter.convert)))
        case Some(DataForm.Double) =>
          new DoubleFieldBuilder(ArrayBuffer(elem.map(set.doubleConverter.convert)))
        case Some(DataForm.Int) =>
          new IntFieldBuilder(ArrayBuffer(elem.map(set.intConverter.convert)))
        case Some(DataForm.Long) =>
          new LongFieldBuilder(ArrayBuffer(elem.map(set.longConverter.convert)))
        case _ =>
          new StringFieldBuilder(ArrayBuffer(elem.map(set.stringConverter.convert)))
      }
    })
  }

  /**
    * Switches a data field builder if the previously inferred type is incorrect
    *
    * @param builder previous field builder
    * @param inf new inferred form
    * @param set conversion set
    * @return updated field builder
    */

  private def switch(builder: DataFieldBuilder, inf: FormInference)(implicit set: ConversionSet): DataFieldBuilder = {

    inf.getForm match {
      case Some(DataForm.Boolean) =>
        new BooleanFieldBuilder(ArrayBuffer.range(0, builder.length).map(i => {
          builder(i) match {
            case null | None => Option.empty
            case v => Some(set.booleanConverter.convert(v))
          }
        }))
      case Some(DataForm.LocalDate) =>
        new LocalDateFieldBuilder(ArrayBuffer.range(0, builder.length).map(i => {
          builder(i) match {
            case null | None => Option.empty
            case v => Some(set.localDateConverter.convert(v))
          }
        }))
      case Some(DataForm.LocalDateTime) =>
        new LocalDateTimeFieldBuilder(ArrayBuffer.range(0, builder.length).map(i => {
          builder(i) match {
            case null | None => Option.empty
            case v => Some(set.localDateTimeConverter.convert(v))
          }
        }))
      case Some(DataForm.Double) =>
        new DoubleFieldBuilder(ArrayBuffer.range(0, builder.length).map(i => {
          builder(i) match {
            case null | None => Option.empty
            case v => Some(set.doubleConverter.convert(v))
          }
        }))
      case Some(DataForm.Int) =>
        new IntFieldBuilder(ArrayBuffer.range(0, builder.length).map(i => {
          builder(i) match {
            case null | None => Option.empty
            case v => Some(set.intConverter.convert(v))
          }
        }))
      case Some(DataForm.Long) =>
        new LongFieldBuilder(ArrayBuffer.range(0, builder.length).map(i => {
          builder(i) match {
            case null | None => Option.empty
            case v => Some(set.longConverter.convert(v))
          }
        }))
      case _ => new StringFieldBuilder(ArrayBuffer.range(0, builder.length).map(i => {
        builder(i) match {
          case null | None => Option.empty
          case v => Some(set.stringConverter.convert(v))
        }
      }))
    }
  }
}
