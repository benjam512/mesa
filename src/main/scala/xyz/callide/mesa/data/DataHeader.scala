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

/**
  * Holds metadata about the fields of a data set
  *
  * @param fieldColumn map of field name to column
  * @param fieldName map of column to field name
  */

case class DataHeader(private val fieldColumn: Map[String, Int],
                      private val fieldName: Map[Int, String],
                      private val fieldForm: Map[String, DataForm]) {

  require(fieldColumn.size == fieldName.size, "Column map and name map size mismatch")
  require(fieldColumn.size == fieldForm.size, "Column map and form map size mismatch")

  def column(name: String): Int = fieldColumn(name)

  def getColumn(name: String): Option[Int] = fieldColumn.get(name)

  def name(column: Int): String = fieldName(column)

  def getName(column: Int): Option[String] = fieldName.get(column)

  def form(name: String): DataForm = fieldForm(name)

  def getForm(name: String): Option[DataForm] = fieldForm.get(name)

  /**
    * Provides the size of the header (number of fields)
    *
    * @return number of fields
    */

  def size: Int = fieldColumn.size

  /**
    * Appends the field name to the header
    *
    * @param name the field name to append
    * @return augmented header
    */

  def append(name: String, form: DataForm): DataHeader = {

    require(!fieldColumn.contains(name), s"Field '$name' already exists")
    DataHeader(fieldColumn + ((name, size)),
               fieldName + ((size, name)),
               fieldForm + ((name, form)))
  }

  /**
    * Prepends the field name to the header
    *
    * @param name the field name to prepend
    * @return augmented header
    */

  def prepend(name: String, form: DataForm): DataHeader = {

    require(!fieldColumn.contains(name), s"Field '$name' already exists")
    DataHeader(fieldColumn.map({case (k, v) => (k, v + 1)}) + ((name, 0)),
               fieldName.map({case (k, v) => (k + 1, v)}) + ((0, name)),
               fieldForm + ((name, form)))
  }

  /**
    * Provides the list of field names
    *
    * @return list of field names
    */

  def fieldNames: List[String] = List.range(0, size).map(ind => fieldName(ind))

  /**
    * Checks if the header already contains the provided field name
    *
    * @param name field name to check
    * @return true if the header already contains the name, or false otherwise
    */

  def contains(name: String): Boolean = fieldColumn.keySet.contains(name)

  /**
    * Removes the provided field name from the header
    *
    * @param name field name to remove
    * @return diminished header
    */

  def remove(name: String): DataHeader = {

    val col = fieldColumn.getOrElse(name, throw new IllegalArgumentException(s"Field $name not found"))
    DataHeader((fieldColumn - name).map({case (n, ind) => if (ind > col) (n, ind - 1) else (n, ind)}),
               (fieldName - col).map({case (ind, n) => if (ind > col) (ind - 1, n) else (ind, n)}),
               fieldForm - name)
  }
}

/**
  * Provides static functionality
  */

object DataHeader {

  def apply(info: Seq[(String, DataForm)]): DataHeader = {

    val indexedInfo = info.zipWithIndex
    DataHeader(indexedInfo.map({case ((name, _), ind) => (name, ind)}).toMap,
               indexedInfo.map({case ((name, _), ind) => (ind, name)}).toMap,
               indexedInfo.map({case ((name, form), _) => (name, form)}).toMap)
  }
}
