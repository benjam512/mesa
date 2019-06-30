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

package xyz.callide.mesa.ordering

/**
  * Defines the ordering direction
  */

sealed trait OrderingDirection

/**
  * Provides static functionality
  */

object OrderingDirection {

  /**
    * Ascending ordering
    */

  case object Ascending extends OrderingDirection

  /**
    * Descending ordering
    */

  case object Descending extends OrderingDirection
}
