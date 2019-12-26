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

  def raw(ind: Int): Any = elements(ind)

  def raw(name: String): Any = elements(header.column(name))

  def apply[A](ind: Int)(implicit converter: Converter[A]): A = converter.convert(elements(ind))

  def apply[A](name: String)(implicit converter: Converter[A]): A = apply(header.column(name))

  def get[A](ind: Int)(implicit converter: Converter[A]): Option[A] = {

    elements(ind) match {
      case elem if elem != null => Some(converter.convert(elem))
      case _ => None
    }
  }

  def get[A](name: String)(implicit converter: Converter[A]): Option[A] = get(header.column(name))

  def map[A](f: DataPoint => A): Vector[A] = elements.map(v => f(DataPoint(Option(v))))
}
