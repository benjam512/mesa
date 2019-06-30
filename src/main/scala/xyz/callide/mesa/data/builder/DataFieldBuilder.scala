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

  def apply(ind: Int): Any
  def append(value: String)(implicit set: ConversionSet): Unit
  def length: Int
  def toField: DataField
}

object DataFieldBuilder {

  class BooleanFieldBuilder(values: ArrayBuffer[Boolean]) extends DataFieldBuilder {
    override def apply(ind: Int): Any = values(ind)
    override def append(value: String)(implicit set: ConversionSet): Unit = values += set.booleanConverter.convert(value)
    override def length: Int = values.length
    override def toField: DataField = new BooleanField(values.toArray)
  }
  class LocalDateFieldBuilder(values: ArrayBuffer[LocalDate]) extends DataFieldBuilder {
    override def apply(ind: Int): Any = values(ind)
    override def append(value: String)(implicit set: ConversionSet): Unit = values += set.localDateConverter.convert(value)
    override def length: Int = values.length
    override def toField: DataField = new LocalDateField(values.toArray)
  }
  class LocalDateTimeFieldBuilder(values: ArrayBuffer[LocalDateTime]) extends DataFieldBuilder {
    override def apply(ind: Int): Any = values(ind)
    override def append(value: String)(implicit set: ConversionSet): Unit = values += set.localDateTimeConverter.convert(value)
    override def length: Int = values.length
    override def toField: DataField = new LocalDateTimeField(values.toArray)
  }
  class DoubleFieldBuilder(values: ArrayBuffer[Double]) extends DataFieldBuilder {
    override def apply(ind: Int): Any = values(ind)
    override def append(value: String)(implicit set: ConversionSet): Unit = values += set.doubleConverter.convert(value)
    override def length: Int = values.length
    override def toField: DataField = new DoubleField(values.toArray)
  }
  class IntFieldBuilder(values: ArrayBuffer[Int]) extends DataFieldBuilder {
    override def apply(ind: Int): Any = values(ind)
    override def append(value: String)(implicit set: ConversionSet): Unit = values += set.intConverter.convert(value)
    override def length: Int = values.length
    override def toField: DataField = new IntField(values.toArray)
  }
  class LongFieldBuilder(values: ArrayBuffer[Long]) extends DataFieldBuilder {
    override def apply(ind: Int): Any = values(ind)
    override def append(value: String)(implicit set: ConversionSet): Unit = values += set.longConverter.convert(value)
    override def length: Int = values.length
    override def toField: DataField = new LongField(values.toArray)
  }
  class StringFieldBuilder(values: ArrayBuffer[String]) extends DataFieldBuilder {
    override def apply(ind: Int): Any = values(ind)
    override def append(value: String)(implicit set: ConversionSet): Unit = values += set.stringConverter.convert(value)
    override def length: Int = values.length
    override def toField: DataField = new StringField(values.toArray)
  }
}