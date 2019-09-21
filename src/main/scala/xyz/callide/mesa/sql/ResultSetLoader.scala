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

import java.sql.{ResultSet, ResultSetMetaData}
import java.time.{LocalDate, LocalDateTime}

import xyz.callide.mesa.data.builder.DataFieldBuilder
import xyz.callide.mesa.data.builder.DataFieldBuilder.{BooleanFieldBuilder, DoubleFieldBuilder, IntFieldBuilder, LocalDateFieldBuilder, LocalDateTimeFieldBuilder, LongFieldBuilder, StringFieldBuilder}
import xyz.callide.mesa.data.conversion.ConversionSet
import xyz.callide.mesa.data.{DataForm, DataHeader, DataSet}

import scala.collection.mutable.ArrayBuffer

object ResultSetLoader {

  def read(results: ResultSet)(implicit set: ConversionSet): DataSet = {

    val metadata = results.getMetaData
    val numCols = metadata.getColumnCount

    val builders = initBuilders(metadata)

    while (results.next) {
      for (i <- 1 to numCols) {
        builders(i-1).append(results.getString(i))
      }
    }

    DataSet(extractHeader(metadata), builders.map(_.toField))
  }

  private def initBuilders(metadata: ResultSetMetaData): Vector[DataFieldBuilder] = {

    Vector.range(1, metadata.getColumnCount + 1).map(col => {
      getDataForm(metadata.getColumnType(col)) match {
        case Some(DataForm.Boolean) => new BooleanFieldBuilder(ArrayBuffer.empty[Boolean])
        case Some(DataForm.LocalDate) => new LocalDateFieldBuilder(ArrayBuffer.empty[LocalDate])
        case Some(DataForm.LocalDateTime) => new LocalDateTimeFieldBuilder(ArrayBuffer.empty[LocalDateTime])
        case Some(DataForm.Double) => new DoubleFieldBuilder(ArrayBuffer.empty[Double])
        case Some(DataForm.Int) => new IntFieldBuilder(ArrayBuffer.empty[Int])
        case Some(DataForm.Long) => new LongFieldBuilder(ArrayBuffer.empty[Long])
        case _ => new StringFieldBuilder(ArrayBuffer.empty[String])
      }
    })
  }

  private def extractHeader(metadata: ResultSetMetaData): DataHeader = {

    DataHeader(Vector.range(1, metadata.getColumnCount + 1).map(col => {
      val name = metadata.getColumnName(col)
      val form = getDataForm(metadata.getColumnType(col)).getOrElse(DataForm.String)
      (name, form)
    }))
  }

  private def getDataForm(code: Int): Option[DataForm] = {

    code match {
      case 16 => Some(DataForm.Boolean)
      case 91 => Some(DataForm.LocalDate)
      case 93 => Some(DataForm.LocalDateTime)
      case 6 | 7 | 8 => Some(DataForm.Double)
      case 4 | 5 => Some(DataForm.Int)
      case -5 => Some(DataForm.Long)
      case 12 => Some(DataForm.String)
      case _ => None
    }
  }
}
