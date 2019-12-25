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

import xyz.callide.mesa.data.conversion.ConversionSet

import scala.util.{Success, Try}

/**
  * Represents the type of data
  */

trait DataForm {

  def name: String
}

object DataForm {

  case object Boolean extends DataForm {
    override def name: String = "Boolean"
  }
  case object LocalDate extends DataForm {
    override def name: String = "LocalDate"
  }
  case object LocalDateTime extends DataForm {
    override def name: String = "LocalDateTime"
  }
  case object Double extends DataForm {
    override def name: String = "Double"
  }
  case object Int extends DataForm {
    override def name: String = "Int"
  }
  case object Long extends DataForm {
    override def name: String = "Long"
  }
  case object String extends DataForm {
    override def name: String = "String"
  }

  class FormInference(set: ConversionSet) {

    private var form: Option[DataForm] = None

    def getForm: Option[DataForm] = form

    def infer(input: String): Unit = {

      if (input != null && input.nonEmpty && !input.forall(c => c == ' ')) {
        tryLocalDateTime(input)
      } else if (form.isEmpty) {
        form = Some(LocalDateTime)
      }
    }

    private def tryLocalDateTime(input: String): Unit = {
      Try(set.localDateTimeConverter.convert(input)) match {
        case Success(_) => form = Some(LocalDateTime)
        case _ => tryLocalDate(input)
      }
    }

    private def tryLocalDate(input: String): Unit = {
      Try(set.localDateConverter.convert(input)) match {
        case Success(_) => form = Some(LocalDate)
        case _ => tryBoolean(input)
      }
    }

    private def tryBoolean(input: String): Unit = {
      Try(set.booleanConverter.convert(input)) match {
        case Success(_) => form = Some(Boolean)
        case _ => tryInt(input)
      }
    }

    private def tryInt(input: String): Unit = {
      Try(set.intConverter.convert(input)) match {
        case Success(_) => form = Some(Int)
        case _ => tryLong(input)
      }
    }

    private def tryLong(input: String): Unit = {
      Try(set.longConverter.convert(input)) match {
        case Success(_) => form = Some(Long)
        case _ => tryDouble(input)
      }
    }

    private def tryDouble(input: String): Unit = {
      Try(set.doubleConverter.convert(input)) match {
        case Success(_) => form = Some(Double)
        case _ => form = Some(String)
      }
    }
  }
}

