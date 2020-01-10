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

  /**
    * Data type being held
    */

  type DataType

  /**
    * Buffer of values
    */

  val values: ArrayBuffer[Option[DataType]]

  /**
    * Gets the raw value at the specified index
    *
    * @param ind value index
    * @return value at specified index
    */

  def apply(ind: Int): Any = values(ind)

  /**
    * Converts the provided string to the specified type
    *
    * @param s the string to convert
    * @param set the implicit conversion set
    * @return converted value
    */

  def convert(s: String)(implicit set: ConversionSet): DataType

  /**
    * Appends the provided string value to the builder
    *
    * @param value the string value to append
    * @param set the implicit conversion set
    */

  def append(value: String)(implicit set: ConversionSet): Unit = {
    if (value == null || value == "" || value.forall(c => c == ' ')) {
      values += Option.empty[DataType]
    } else values += Some(convert(value))
  }

  /**
    * Builder length
    *
    * @return length
    */

  def length: Int = values.length

  /**
    * Converts the builder to a field instance
    *
    * @return converted data field
    */

  def toField: DataField
}

/**
  * Provides static functionality
  */

object DataFieldBuilder {

  /**
    * Boolean builder
    *
    * @param values buffer of optional Boolean values
    */

  class BooleanFieldBuilder(val values: ArrayBuffer[Option[Boolean]]) extends DataFieldBuilder {
    override type DataType = Boolean
    override def convert(s: String)(implicit set: ConversionSet): DataType = set.booleanConverter.convert(s)
    override def toField: DataField = new BooleanField(values.toArray)
  }

  /**
    * LocalDate builder
    *
    * @param values buffer of optional LocalDate values
    */

  class LocalDateFieldBuilder(val values: ArrayBuffer[Option[LocalDate]]) extends DataFieldBuilder {
    override type DataType = LocalDate
    override def convert(s: String)(implicit set: ConversionSet): LocalDate = set.localDateConverter.convert(s)
    override def toField: DataField = new LocalDateField(values.toArray)
  }

  /**
    * LocalDateTime builder
    *
    * @param values buffer of optional LocalDateTime values
    */

  class LocalDateTimeFieldBuilder(val values: ArrayBuffer[Option[LocalDateTime]]) extends DataFieldBuilder {
    override type DataType = LocalDateTime
    override def convert(s: String)(implicit set: ConversionSet): LocalDateTime = set.localDateTimeConverter.convert(s)
    override def toField: DataField = new LocalDateTimeField(values.toArray)
  }

  /**
    * Double builder
    *
    * @param values buffer of optional Double values
    */

  class DoubleFieldBuilder(val values: ArrayBuffer[Option[Double]]) extends DataFieldBuilder {
    override type DataType = Double
    override def convert(s: String)(implicit set: ConversionSet): Double = set.doubleConverter.convert(s)
    override def toField: DataField = new DoubleField(values.toArray)
  }

  /**
    * Int builder
    *
    * @param values buffer of optional Int values
    */

  class IntFieldBuilder(val values: ArrayBuffer[Option[Int]]) extends DataFieldBuilder {
    override type DataType = Int
    override def convert(s: String)(implicit set: ConversionSet): Int = set.intConverter.convert(s)
    override def toField: DataField = new IntField(values.toArray)
  }

  /**
    * Long builder
    *
    * @param values buffer of optional Long values
    */

  class LongFieldBuilder(val values: ArrayBuffer[Option[Long]]) extends DataFieldBuilder {
    override type DataType = Long
    override def convert(s: String)(implicit set: ConversionSet): Long = set.longConverter.convert(s)
    override def toField: DataField = new LongField(values.toArray)
  }

  /**
    * String builder
    *
    * @param values buffer of optional String values
    */

  class StringFieldBuilder(val values: ArrayBuffer[Option[String]]) extends DataFieldBuilder {
    override type DataType = String
    override def convert(s: String)(implicit set: ConversionSet): String = set.stringConverter.convert(s)
    override def toField: DataField = new StringField(values.toArray)
  }
}
