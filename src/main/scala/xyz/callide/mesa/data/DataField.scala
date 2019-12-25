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

trait DataField {

  type DataType

  val form: DataForm

  protected val elements: Array[Option[DataType]]

  lazy val hasMissingValues: Boolean = elements.exists(elem => elem.isEmpty)

  def point(ind: Int): DataPoint = DataPoint(elements(ind))

  def raw(ind: Int): Any = elements(ind).orNull

  def points: Vector[DataPoint] = elements.map(elem => DataPoint(elem)).toVector

  def extract[B](implicit converter: Converter[B]): Vector[B] = {

    elements.flatMap(elem => elem.map(converter.convert)).toVector
  }

  def extract[B](default: B)(implicit converter: Converter[B]): Vector[B] = {

    elements.map(elem => {
      elem.map(converter.convert).getOrElse(default)
    }).toVector
  }

  def length: Int = elements.length

  def subset(indices: Seq[Int]): DataField
}

class BooleanField(protected val elements: Array[Option[Boolean]]) extends DataField {
  override type DataType = Boolean
  override val form: DataForm = DataForm.Boolean
  override def subset(indices: Seq[Int]): BooleanField = new BooleanField(indices.map(ind => elements(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: BooleanField => field.elements.sameElements(elements)
    case _ => false
  }
}
object BooleanField {
  def apply(elements: Array[Boolean]): BooleanField = new BooleanField(elements.map(elem => Some(elem)))
}

class LocalDateField(protected val elements: Array[Option[LocalDate]]) extends DataField {
  override type DataType = LocalDate
  override val form: DataForm = DataForm.LocalDate
  override def subset(indices: Seq[Int]): LocalDateField = new LocalDateField(indices.map(ind => elements(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: LocalDateField => field.elements.sameElements(elements)
    case _ => false
  }
}
object LocalDateField {
  def apply(elements: Array[LocalDate]): LocalDateField = new LocalDateField(elements.map(elem => Some(elem)))
}

class LocalDateTimeField(protected val elements: Array[Option[LocalDateTime]]) extends DataField {
  override type DataType = LocalDateTime
  override val form: DataForm = DataForm.LocalDateTime
  override def subset(indices: Seq[Int]): LocalDateTimeField = new LocalDateTimeField(indices.map(ind => elements(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: LocalDateTimeField => field.elements.sameElements(elements)
    case _ => false
  }
}
object LocalDateTimeField {
  def apply(elements: Array[LocalDateTime]): LocalDateTimeField = new LocalDateTimeField(elements.map(elem => Some(elem)))
}

class DoubleField(protected val elements: Array[Option[Double]]) extends DataField {
  override type DataType = Double
  override val form: DataForm = DataForm.Double
  override def subset(indices: Seq[Int]): DoubleField = new DoubleField(indices.map(ind => elements(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: DoubleField => field.elements.sameElements(elements)
    case _ => false
  }
}
object DoubleField {
  def apply(elements: Array[Double]): DoubleField = new DoubleField(elements.map(elem => Some(elem)))
}

class IntField(protected val elements: Array[Option[Int]]) extends DataField {
  override type DataType = Int
  override val form: DataForm = DataForm.Int
  override def subset(indices: Seq[Int]): IntField = new IntField(indices.map(ind => elements(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: IntField => field.elements.sameElements(elements)
    case _ => false
  }
}
object IntField {
  def apply(elements: Array[Int]): IntField = new IntField(elements.map(elem => Some(elem)))
}

class LongField(protected val elements: Array[Option[Long]]) extends DataField {
  override type DataType = Long
  override val form: DataForm = DataForm.Long
  override def subset(indices: Seq[Int]): LongField = new LongField(indices.map(ind => elements(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: LongField => field.elements.sameElements(elements)
    case _ => false
  }
}
object LongField {
  def apply(elements: Array[Long]): LongField = new LongField(elements.map(elem => Some(elem)))
}

class StringField(protected val elements: Array[Option[String]]) extends DataField {
  override type DataType = String
  override val form: DataForm = DataForm.String
  override def subset(indices: Seq[Int]): StringField = new StringField(indices.map(ind => elements(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: StringField => field.elements.sameElements(elements)
    case _ => false
  }
}
object StringField {
  def apply(elements: Array[String]): StringField = new StringField(elements.map(elem => Some(elem)))
}
