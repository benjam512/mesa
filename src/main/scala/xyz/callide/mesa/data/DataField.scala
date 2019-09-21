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

  def form: DataForm

  def point(ind: Int): DataPoint = DataPoint(raw(ind))

  def raw(ind: Int): Any

  def apply[B](ind: Int)(implicit converter: Converter[B]): B

  def mapTo[B](implicit converter: Converter[B]): Vector[B]

  def length: Int

  def subset(indices: Seq[Int]): DataField
}

class BooleanField(private val values: Array[Boolean]) extends DataField {
  override def form: DataForm = DataForm.Boolean
  override def raw(ind: Int): Any = values(ind)
  override def apply[B](ind: Int)(implicit converter: Converter[B]): B = converter.convert(values(ind))
  override def mapTo[B](implicit converter: Converter[B]): Vector[B] = values.map(converter.convert).toVector
  override def length: Int = values.length
  override def subset(indices: Seq[Int]): BooleanField = new BooleanField(indices.map(ind => values(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: BooleanField => field.values.sameElements(values)
    case _ => false
  }
}

class LocalDateField(private val values: Array[LocalDate]) extends DataField {
  override def form: DataForm = DataForm.LocalDate
  override def raw(ind: Int): Any = values(ind)
  override def apply[B](ind: Int)(implicit converter: Converter[B]): B = converter.convert(values(ind))
  override def mapTo[B](implicit converter: Converter[B]): Vector[B] = values.map(converter.convert).toVector
  override def length: Int = values.length
  override def subset(indices: Seq[Int]): LocalDateField = new LocalDateField(indices.map(ind => values(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: LocalDateField => field.values.sameElements(values)
    case _ => false
  }
}

class LocalDateTimeField(private val values: Array[LocalDateTime]) extends DataField {
  override def form: DataForm = DataForm.LocalDateTime
  override def raw(ind: Int): Any = values(ind)
  override def apply[B](ind: Int)(implicit converter: Converter[B]): B = converter.convert(values(ind))
  override def mapTo[B](implicit converter: Converter[B]): Vector[B] = values.map(converter.convert).toVector
  override def length: Int = values.length
  override def subset(indices: Seq[Int]): LocalDateTimeField = new LocalDateTimeField(indices.map(ind => values(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: LocalDateTimeField => field.values.sameElements(values)
    case _ => false
  }
}

class DoubleField(private val values: Array[Double]) extends DataField {
  override def form: DataForm = DataForm.Double
  override def raw(ind: Int): Any = values(ind)
  override def apply[B](ind: Int)(implicit converter: Converter[B]): B = converter.convert(values(ind))
  override def mapTo[B](implicit converter: Converter[B]): Vector[B] = values.map(converter.convert).toVector
  override def length: Int = values.length
  override def subset(indices: Seq[Int]): DoubleField = new DoubleField(indices.map(ind => values(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: DoubleField => field.values.sameElements(values)
    case _ => false
  }
}

class IntField(private val values: Array[Int]) extends DataField {
  override def form: DataForm = DataForm.Int
  override def raw(ind: Int): Any = values(ind)
  override def apply[B](ind: Int)(implicit converter: Converter[B]): B = converter.convert(values(ind))
  override def mapTo[B](implicit converter: Converter[B]): Vector[B] = values.map(converter.convert).toVector
  override def length: Int = values.length
  override def subset(indices: Seq[Int]): IntField = new IntField(indices.map(ind => values(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: IntField => field.values.sameElements(values)
    case _ => false
  }
}

class LongField(private val values: Array[Long]) extends DataField {
  override def form: DataForm = DataForm.Long
  override def raw(ind: Int): Any = values(ind)
  override def apply[B](ind: Int)(implicit converter: Converter[B]): B = converter.convert(values(ind))
  override def mapTo[B](implicit converter: Converter[B]): Vector[B] = values.map(converter.convert).toVector
  override def length: Int = values.length
  override def subset(indices: Seq[Int]): LongField = new LongField(indices.map(ind => values(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: LongField => field.values.sameElements(values)
    case _ => false
  }
}

class StringField(private val values: Array[String]) extends DataField {
  override def form: DataForm = DataForm.String
  override def raw(ind: Int): Any = values(ind)
  override def apply[B](ind: Int)(implicit converter: Converter[B]): B = converter.convert(values(ind))
  override def mapTo[B](implicit converter: Converter[B]): Vector[B] = values.map(converter.convert).toVector
  override def length: Int = values.length
  override def subset(indices: Seq[Int]): StringField = new StringField(indices.map(ind => values(ind)).toArray)
  override def equals(obj: Any): Boolean = obj match {
    case field: StringField => field.values.sameElements(values)
    case _ => false
  }
}
