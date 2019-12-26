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

package xyz.callide.mesa.data.conversion

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import xyz.callide.mesa.exception.InvalidConversionException

/**
  * Defines conversions between types
  *
  * @tparam A the result type
  */

trait Converter[A] {

  /**
    * Converts the provided value to the desired type
    *
    * @param value input value
    * @return result type
    */

  def convert(value: Any): A
}

/**
  * Provides static functionality
  */

object Converter {

  /**
    * Default converter for Booleans
    */

  implicit object DefaultBooleanConverter extends Converter[Boolean] {
    override def convert(value: Any): Boolean = value match {
      case v: Boolean => v
      case v: Int => if (v == 0) false else if (v == 1) true else throw InvalidConversionException[Boolean](v)
      case v: Long => if (v == 0) false else if (v == 1) true else throw InvalidConversionException[Boolean](v)
      case v: Double => if (v == 0.0) false else if (v == 1.0) true else throw InvalidConversionException[Boolean](v)
      case v: String => v match {
        case "false" | "False" | "FALSE" | "0" => false
        case "true" | "True" | "TRUE" | "1" => true
        case _ => throw InvalidConversionException[Boolean](v)
      }
      case v => throw InvalidConversionException[Boolean](v)
    }
  }

  /**
    * Default converter for LocalDates
    */

  implicit object DefaultLocalDateConverter extends Converter[LocalDate] {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    override def convert(value: Any): LocalDate = value match {
      case v: LocalDate => v
      case v: LocalDateTime => v.toLocalDate
      case v: String => LocalDate.parse(v, formatter)
      case v => throw InvalidConversionException[LocalDate](v)
    }
  }

  /**
    * Default converter for LocalDateTimes
    */

  implicit object DefaultLocalDateTimeConverter extends Converter[LocalDateTime] {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    override def convert(value: Any): LocalDateTime = value match {
      case v: LocalDateTime => v
      case v: String => LocalDateTime.parse(v.substring(0, 19), formatter)
      case v => throw InvalidConversionException[LocalDateTime](v)
    }
  }

  /**
    * Default converter for Doubles
    */

  implicit object DefaultDoubleConverter extends Converter[Double] {
    override def convert(value: Any): Double = value match {
      case v: Double => v
      case v: Boolean => if (v) 1.0 else 0.0
      case v: Int => v.toDouble
      case v: Long => v.toDouble
      case v: String => v.toDouble
      case v => throw InvalidConversionException[Double](v)
    }
  }

  /**
    * Default converter for Ints
    */

  implicit object DefaultIntConverter extends Converter[Int] {
    override def convert(value: Any): Int = value match {
      case v: Int => v
      case v: Boolean => if (v) 1 else 0
      case v: Long => v.toInt
      case v: Double => v.toInt
      case v: String => v.toInt
      case v => throw InvalidConversionException[Int](v)
    }
  }

  /**
    * Default converter for Longs
    */

  implicit object DefaultLongConverter extends Converter[Long] {
    override def convert(value: Any): Long = value match {
      case v: Long => v
      case v: Boolean => if (v) 1 else 0
      case v: Int => v.toLong
      case v: Double => v.toLong
      case v: String => v.toLong
      case v => throw InvalidConversionException[Long](v)
    }
  }

  /**
    * Default converter for Strings
    */

  implicit object DefaultStringConverter extends Converter[String] {
    override def convert(value: Any): String = if (value == null) "" else value.toString
  }
}
