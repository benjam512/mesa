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

package xyz.callide.mesa.data

import xyz.callide.mesa.data.conversion.Converter

/**
  * Represents a row of data in a data set
  *
  * @param header corresponding data set header
  * @param elements row elements
  */

case class DataRow(header: DataHeader, elements: Vector[Any]) {

  /**
    * Provides the raw value at the specified column index
    *
    * @param ind the column index
    * @return corresponding raw value
    */

  def raw(ind: Int): Any = elements(ind)

  /**
    * Provides the raw value at the specified field name
    *
    * @param name the field name
    * @return corresponding raw value
    */

  def raw(name: String): Any = elements(header.column(name))

  /**
    * Converts the specified value to the desired data type, making the assumption that the value exists. Otherwise,
    * and exception is thrown.
    *
    * @param ind the column index
    * @param converter implicit converter
    * @tparam A desired data type
    * @return converted value
    */

  def apply[A](ind: Int)(implicit converter: Converter[A]): A = converter.convert(elements(ind))

  /**
    * Converts the specified value to the desired data type, making the assumption that the value exists. Otherwise,
    * and exception is thrown.
    *
    * @param name the field name
    * @param converter implicit converter
    * @tparam A desired data type
    * @return converted value
    */

  def apply[A](name: String)(implicit converter: Converter[A]): A = apply(header.column(name))

  /**
    * Converts the specified value to the desired data type, retaining its optional status.
    *
    * @param ind the column index
    * @param converter implicit converter
    * @tparam A desired data type
    * @return converted value
    */

  def get[A](ind: Int)(implicit converter: Converter[A]): Option[A] = {

    elements(ind) match {
      case elem if elem != null => Some(converter.convert(elem))
      case _ => None
    }
  }

  /**
    * Converts the specified value to the desired data type, retaining its optional status.
    *
    * @param name the field name
    * @param converter implicit converter
    * @tparam A desired data type
    * @return converted value
    */

  def get[A](name: String)(implicit converter: Converter[A]): Option[A] = get(header.column(name))

  /**
    * Applies the given function to each element of the row, wrapped as a data point
    *
    * @param f function mapping a data point to a value
    * @tparam A desired data type
    * @return vector of mapped values
    */

  def map[A](f: DataPoint => A): Vector[A] = elements.map(v => f(DataPoint(Option(v))))
}

/**
  * Iterator for rows of a data set
  *
  * @param header the data set header
  * @param fields the data set fields
  */

class DataRowIterator private [data] (header: DataHeader, fields: Vector[DataField]) extends Iterator[DataRow] {

  private var iter = -1
  override val length: Int = fields.head.length
  require(fields.forall(field => field.length == length))

  override def hasNext: Boolean = {iter + 1 < length}

  override def next(): DataRow = {

    iter += 1
    DataRow(header, fields.map(field => field.raw(iter)))
  }
}
