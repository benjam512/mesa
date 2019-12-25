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

package xyz.callide.mesa.exception

import scala.reflect.ClassTag

/**
  * Gets thrown when attempting to make an invalid conversion between data types
  */

case class InvalidConversionException[A](input: Any)(implicit val tag: ClassTag[A]) extends RuntimeException {

  override def getMessage: String = s"Cannot convert '${input.toString}' to type '${tag.toString}'"

}
