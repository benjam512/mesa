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

import java.time.{LocalDate, LocalDateTime}

import xyz.callide.mesa.data.conversion.Converter.{DefaultBooleanConverter, DefaultDoubleConverter, DefaultIntConverter, DefaultLocalDateConverter, DefaultLocalDateTimeConverter, DefaultLongConverter, DefaultStringConverter}

/**
  * Holds all default converters
  */

object DefaultConversionSet extends ConversionSet {
  override val localDateTimeConverter: Converter[LocalDateTime] = DefaultLocalDateTimeConverter
  override val localDateConverter: Converter[LocalDate] = DefaultLocalDateConverter
  override val booleanConverter: Converter[Boolean] = DefaultBooleanConverter
  override val doubleConverter: Converter[Double] = DefaultDoubleConverter
  override val intConverter: Converter[Int] = DefaultIntConverter
  override val longConverter: Converter[Long] = DefaultLongConverter
  override val stringConverter: Converter[String] = DefaultStringConverter
}
