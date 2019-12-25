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

package xyz.callide.mesa.data.builder

import java.time.{LocalDate, LocalDateTime}

import xyz.callide.mesa.data.conversion.ConversionSet
import xyz.callide.mesa.data.{BooleanField, DataField, DoubleField, IntField, LocalDateField, LocalDateTimeField, LongField, StringField}

import scala.collection.mutable.ArrayBuffer

/**
  * Provides functionality for building fields dynamically
  */

trait DataFieldBuilder {

  type DataType
  val values: ArrayBuffer[Option[DataType]]
  def apply(ind: Int): Any = values(ind)
  def convert(s: String)(implicit set: ConversionSet): DataType
  def append(value: String)(implicit set: ConversionSet): Unit = {
    if (value == null || value == "" || value.forall(c => c == ' ')) {
      values += Option.empty[DataType]
    } else values += Some(convert(value))
  }
  def length: Int = values.length
  def toField: DataField
}

object DataFieldBuilder {

  class BooleanFieldBuilder(val values: ArrayBuffer[Option[Boolean]]) extends DataFieldBuilder {
    override type DataType = Boolean
    override def convert(s: String)(implicit set: ConversionSet): DataType = set.booleanConverter.convert(s)
    override def toField: DataField = new BooleanField(values.toArray)
  }
  class LocalDateFieldBuilder(val values: ArrayBuffer[Option[LocalDate]]) extends DataFieldBuilder {
    override type DataType = LocalDate
    override def convert(s: String)(implicit set: ConversionSet): LocalDate = set.localDateConverter.convert(s)
    override def toField: DataField = new LocalDateField(values.toArray)
  }
  class LocalDateTimeFieldBuilder(val values: ArrayBuffer[Option[LocalDateTime]]) extends DataFieldBuilder {
    override type DataType = LocalDateTime
    override def convert(s: String)(implicit set: ConversionSet): LocalDateTime = set.localDateTimeConverter.convert(s)
    override def toField: DataField = new LocalDateTimeField(values.toArray)
  }
  class DoubleFieldBuilder(val values: ArrayBuffer[Option[Double]]) extends DataFieldBuilder {
    override type DataType = Double
    override def convert(s: String)(implicit set: ConversionSet): Double = set.doubleConverter.convert(s)
    override def toField: DataField = new DoubleField(values.toArray)
  }
  class IntFieldBuilder(val values: ArrayBuffer[Option[Int]]) extends DataFieldBuilder {
    override type DataType = Int
    override def convert(s: String)(implicit set: ConversionSet): Int = set.intConverter.convert(s)
    override def toField: DataField = new IntField(values.toArray)
  }
  class LongFieldBuilder(val values: ArrayBuffer[Option[Long]]) extends DataFieldBuilder {
    override type DataType = Long
    override def convert(s: String)(implicit set: ConversionSet): Long = set.longConverter.convert(s)
    override def toField: DataField = new LongField(values.toArray)
  }
  class StringFieldBuilder(val values: ArrayBuffer[Option[String]]) extends DataFieldBuilder {
    override type DataType = String
    override def convert(s: String)(implicit set: ConversionSet): String = set.stringConverter.convert(s)
    override def toField: DataField = new StringField(values.toArray)
  }
}