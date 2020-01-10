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

import java.time.{LocalDate, LocalDateTime}

import xyz.callide.mesa.data.conversion.Converter

/**
  * Represents a field (column) of data in a DataSet
  */

sealed trait DataField {

  /**
    * The actual data type being held
    */

  type DataType

  /**
    * Matching data form marker
    */

  val form: DataForm

  /**
    * The field elements, stored as options
    */

  protected val elements: Array[Option[DataType]]

  /**
    * Indicates whether or not this field contains any missing values
    */

  lazy val hasMissingValues: Boolean = elements.exists(elem => elem.isEmpty)

  /**
    * Wraps the specified element as a data point
    *
    * @param ind the element index
    * @return specified data point
    */

  def point(ind: Int): DataPoint = DataPoint(elements(ind))

  /**
    * Provides the raw value of the specified element
    *
    * @param ind the element index
    * @return specified raw element
    */

  def raw(ind: Int): Any = elements(ind).orNull

  /**
    * Transforms the field elements into a vector of data points
    *
    * @return data point vector
    */

  def points: Vector[DataPoint] = elements.map(elem => DataPoint(elem)).toVector

  /**
    * Extracts the field elements into a vector of the specified type. Note that this method silently drops any
    * missing values in the field.
    *
    * @param converter implicit converter for the desired type
    * @tparam B desired element type
    * @return vector of extracted elements
    */

  def extract[B](implicit converter: Converter[B]): Vector[B] = {

    elements.flatMap(elem => elem.map(converter.convert)).toVector
  }

  /**
    * Extracts the field elements into a vector of the specified type, while using the provided default value to fill
    * in any missing field values
    *
    * @param default the default value for any missing data
    * @param converter implicit converter for the desired type
    * @tparam B desired element type
    * @return vector of extracted elements
    */

  def extract[B](default: B)(implicit converter: Converter[B]): Vector[B] = {

    elements.map(elem => {
      elem.map(converter.convert).getOrElse(default)
    }).toVector
  }

  /**
    * Provides the number of elements in the field
    *
    * @return field length
    */

  def length: Int = elements.length

  /**
    * Subsets the field to the specified indices
    *
    * @param indices the indices to retain in the resulting field
    * @return subset field
    */

  def subset(indices: Seq[Int]): DataField

  /**
    * Checks for equality between this field and the provided object
    *
    * @param obj the object to compare
    * @return true if this field exactly matches the provided object, and false otherwise
    */

  override def equals(obj: Any): Boolean = {

    obj match {
      case field: DataField => field.elements.sameElements(elements)
      case _ => false
    }
  }
}

/**
  * Specialized Boolean field
  *
  * @param elements optional Boolean elements
  */

class BooleanField(protected val elements: Array[Option[Boolean]]) extends DataField {
  override type DataType = Boolean
  override val form: DataForm = DataForm.Boolean
  override def subset(indices: Seq[Int]): BooleanField = new BooleanField(indices.map(ind => elements(ind)).toArray)
}
object BooleanField {
  def apply(elements: Boolean*): BooleanField = new BooleanField(elements.map(elem => Some(elem)).toArray)
}

/**
  * Specialized LocalDate field
  *
  * @param elements optional LocalDate elements
  */

class LocalDateField(protected val elements: Array[Option[LocalDate]]) extends DataField {
  override type DataType = LocalDate
  override val form: DataForm = DataForm.LocalDate
  override def subset(indices: Seq[Int]): LocalDateField = new LocalDateField(indices.map(ind => elements(ind)).toArray)
}
object LocalDateField {
  def apply(elements: LocalDate*): LocalDateField = new LocalDateField(elements.map(elem => Some(elem)).toArray)
}

/**
  * Specialized LocalDateTime field
  *
  * @param elements optional LocalDateTime elements
  */

class LocalDateTimeField(protected val elements: Array[Option[LocalDateTime]]) extends DataField {
  override type DataType = LocalDateTime
  override val form: DataForm = DataForm.LocalDateTime
  override def subset(indices: Seq[Int]): LocalDateTimeField = {
    new LocalDateTimeField(indices.map(ind => elements(ind)).toArray)
  }
}
object LocalDateTimeField {
  def apply(elements: LocalDateTime*): LocalDateTimeField = {
    new LocalDateTimeField(elements.map(elem => Some(elem)).toArray)
  }
}

/**
  * Specialized Double field
  *
  * @param elements optional Double elements
  */

class DoubleField(protected val elements: Array[Option[Double]]) extends DataField {
  override type DataType = Double
  override val form: DataForm = DataForm.Double
  override def subset(indices: Seq[Int]): DoubleField = new DoubleField(indices.map(ind => elements(ind)).toArray)
}
object DoubleField {
  def apply(elements: Double*): DoubleField = new DoubleField(elements.map(elem => Some(elem)).toArray)
}

/**
  * Specialized Int field
  *
  * @param elements optional Int elements
  */

class IntField(protected val elements: Array[Option[Int]]) extends DataField {
  override type DataType = Int
  override val form: DataForm = DataForm.Int
  override def subset(indices: Seq[Int]): IntField = new IntField(indices.map(ind => elements(ind)).toArray)
}
object IntField {
  def apply(elements: Int*): IntField = new IntField(elements.map(elem => Some(elem)).toArray)
}

/**
  * Specialized Long field
  *
  * @param elements optional Long elements
  */

class LongField(protected val elements: Array[Option[Long]]) extends DataField {
  override type DataType = Long
  override val form: DataForm = DataForm.Long
  override def subset(indices: Seq[Int]): LongField = new LongField(indices.map(ind => elements(ind)).toArray)
}
object LongField {
  def apply(elements: Long*): LongField = new LongField(elements.map(elem => Some(elem)).toArray)
}

/**
  * Specialized String field
  *
  * @param elements optional String elements
  */

class StringField(protected val elements: Array[Option[String]]) extends DataField {
  override type DataType = String
  override val form: DataForm = DataForm.String
  override def subset(indices: Seq[Int]): StringField = new StringField(indices.map(ind => elements(ind)).toArray)
}
object StringField {
  def apply(elements: String*): StringField = new StringField(elements.map(elem => Some(elem)).toArray)
}
