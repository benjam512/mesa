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

import java.io.InputStream

import xyz.callide.mesa.csv.CsvLoader
import xyz.callide.mesa.data.conversion.{ConversionSet, DefaultConversionSet}
import xyz.callide.mesa.ordering.OrderingDirection

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * The main abstraction -- a container for tabular data supporting certain SQL-like operations
  *
  * @param header data header
  * @param fields data fields
  */

case class DataSet(header: DataHeader, fields: Vector[DataField]) {

  // validate input
  require(header.size == fields.length, "Length mismatch between header and fields")
  require(fields.tail.forall(field => field.length == fields.head.length), "Inconsistent row count")

  /**
    * Provides the number of records contained in the data set
    *
    * @return number of records
    */

  def count: Int = fields.head.length

  /**
    * Provides the indicated field
    *
    * @param name name of the desired field
    * @return indicated field
    */

  def apply(name: String): DataField = fields(header.fieldColumn(name))

  /**
    * Provides the indicated row
    *
    * @param ind the row index
    * @return corresponding row
    */

  def row(ind: Int): DataRow = DataRow(header, fields.map(field => field.raw(ind)))

  /**
    * Maps the rows of this data set to the specified output
    *
    * @param f mapping function
    * @tparam A result type
    * @return mapped row output
    */

  def mapRows[A](f: DataRow => A): Vector[A] = Vector.range(0, count).map(i => f(row(i)))

  /**
    * Appends the provided field to the data set
    *
    * @param name the name of the field to append
    * @param field the field to append
    * @return augmented data set
    */

  def append(name: String, field: DataField): DataSet = {

    require(field.length == count, "Field length mismatch")
    new DataSet(header.append(name, field.form), fields :+ field)
  }

  /**
    * Prepends the provided field to the data set
    *
    * @param name the name of the field to prepend
    * @param field the field to prepend
    * @return augmented data set
    */

  def prepend(name: String, field: DataField): DataSet = {

    require(field.length == count, "Field length mismatch")
    new DataSet(header.prepend(name, field.form), field +: fields)
  }

  /**
    * Drops the indicated field from the data set
    *
    * @param name the name of the field to drop
    * @return diminished data set
    */

  def drop(name: String): DataSet = {

    val fieldToDrop = fields(header.fieldColumn(name))
    new DataSet(header.remove(name), fields.filter(field => field != fieldToDrop))
  }

  /**
    * Merges the provided data set with this data set
    *
    * @param other the data set with which to merge
    * @return combined data set
    */

  def merge(other: DataSet): DataSet = {

    require(count == other.count, "Cannot merge data set with different number of records")
    new DataSet(other.header.fieldNames.foldLeft(header)((result, name) => {
      result.append(name, other.header.fieldForm(name))
    }), fields ++ other.fields)
  }

  /**
    * Subsets the data to the specified rows. Note that duplicates are allowed, as the rows are simply mapped to the
    * provided indices in their order.
    *
    * @param indices the row indices to retain
    * @return subset data
    */

  def subset(indices: Seq[Int]): DataSet = {

    require(indices.nonEmpty, "Empty indices")
    copy(fields = fields.map(field => field.subset(indices)))
  }

  /**
    * Selects the specified fields from the data set
    *
    * @param names the field names to select
    * @return data set with selected fields
    */

  def select(names: String*): DataSet = {

    new DataSet(DataHeader(names.map(name => (name, header.fieldForm(name)))),
                names.map(header.fieldColumn).map(ind => fields(ind)).toVector)
  }

  /**
    * Filters the rows of this data set based on the specified condition
    *
    * @param name the name of the field on which to filter
    * @param condition the filter condition
    * @return filtered data set
    */

  def filter(name: String)(condition: DataPoint => Boolean): DataSet = {

    val field = apply(name)
    val indices = Array.range(0, count).filter(i => condition(field.point(i)))
    subset(indices)
  }

  /**
    * Filters the rows of this data set based on the specified condition
    *
    * @param condition the filter condition
    * @return filtered data set
    */

  def filter(condition: DataRow => Boolean): DataSet = {

    val indices = Array.range(0, count).filter(i => condition(row(i)))
    subset(indices)
  }

  /**
    * Orders the data set by the specified field
    *
    * @param name field name on which to order
    * @param direction ordering direction
    * @param transform function converting a data point to the ordering type
    * @param ord implicit ordering
    * @tparam A ordering type
    * @return ordered data set
    */

  def order[A](name: String, direction: OrderingDirection = OrderingDirection.Ascending)
              (transform: DataPoint => A)(implicit ord: Ordering[A]): DataSet = {

    val field = apply(name)
    val indices = Array.range(0, count).map(i => (transform(field.point(i)), i)).sortBy(_._1).map(_._2)

    subset(direction match {
      case OrderingDirection.Ascending => indices
      case OrderingDirection.Descending => indices.reverse
    })
  }

  /**
    * Groups by the specified field
    *
    * @param name name of the field by which to group
    * @param transform transform function for data point
    * @tparam A transform type
    * @return map of grouped key to subset data set
    */

  def group[A](name: String)(transform: DataPoint => A): Map[A, DataSet] = {

    val keyToInd = mutable.HashMap.empty[A, ArrayBuffer[Int]]
    val field = apply(name)
    Array.range(0, count).foreach(ind => {
      val key = transform(field.point(ind))
      keyToInd.get(key) match {
        case Some(inds) => inds += ind
        case _ => keyToInd.update(key, ArrayBuffer(ind))
      }
    })

    keyToInd.map({case (key, inds) => (key, DataSet(header, fields.map(field => field.subset(inds))))}).toMap
  }

  /**
    * Counts the number of records which match the provided condition
    *
    * @param name field name to which the condition applies
    * @param condition the predicate
    * @return number of occurrences
    */

  def count(name: String)(condition: DataPoint => Boolean): Int = {

    val field = apply(name)
    Array.range(0, count).count(i => condition(field.point(i)))
  }

  /**
    * Counts the number of records which match the provided condition
    *
    * @param condition the predicate
    * @return number of occurrences
    */

  def count(condition: DataRow => Boolean): Int = {

    Array.range(0, count).count(i => condition(row(i)))
  }

  /**
    * Prints a preview of the data to the console
    *
    * @param limit number of rows to limit
    */

  def preview(limit: Int = 10): Unit = {

    val maxWidth = 50

    def pad(str: String, width: Int): String = {
      val formatted = str.replace("\n", """\n""")
      val input = if (formatted.length > maxWidth) {
        formatted.substring(0, maxWidth - 3) + "..."
      } else formatted
      val padSize = width - input.length
      val frontSize = padSize / 2
      val backSize = padSize - frontSize
      val front = new String(Array.fill(frontSize)(' '))
      val back = new String(Array.fill(backSize)(' '))
      " " + front + input + back + " "
    }

    val widths = header.fieldNames.map(_.length).toArray

    for (i <- 0 until math.min(limit, count)) {
      val r = row(i)
      r.elements.zipWithIndex.foreach({case (elem, j) =>
        widths(j) = math.min(math.max(widths(j), DataPoint(elem).as[String].length), maxWidth)
      })
    }

    val buffer = new String(Array.fill(widths.sum + header.size * 2)('='))

    println("\n" + buffer)
    header.fieldNames.zipWithIndex.foreach({case (name, j) => print(pad(name, widths(j)))})
    println("\n" + buffer)

    for (i <- 0 until math.min(limit, count)) {
      val r = row(i)
      r.elements.zipWithIndex.foreach({case (elem, j) =>
        print(pad(DataPoint(elem).as[String], widths(j)))
      })
      println()
    }
  }
}

/**
  * Provides static functionality
  */

object DataSet {

  /**
    * Reads in a CSV from file
    *
    * @param path CSV file path
    * @param delimiter column separator
    * @param set conversion set to use
    * @return data set
    */

  def fromCsvFile(path: String, delimiter: Char = ',')
                 (implicit set: ConversionSet = DefaultConversionSet): DataSet = CsvLoader.read(path, delimiter)

  /**
    * Reads in a CSV from a stream
    *
    * @param stream the input stream
    * @param delimiter column separator
    * @param set conversion set to use
    * @return data set
    */

  def fromCsvStream(stream: InputStream, delimiter: Char = ',')
                   (implicit set: ConversionSet = DefaultConversionSet): DataSet = CsvLoader.read(stream, delimiter)

  /**
    * Reads in a CSV from resource
    *
    * @param name the resource name
    * @param delimiter column separator
    * @param set conversion set to use
    * @return data set
    */

  def fromCsvResource(name: String, delimiter: Char = ',')
                     (implicit set: ConversionSet = DefaultConversionSet): DataSet = {

    CsvLoader.read(getClass.getClassLoader.getResourceAsStream(name), delimiter)
  }
}
