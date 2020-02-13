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

package xyz.callide.mesa.file

import com.univocity.parsers.common.{ParsingContext, ResultIterator}
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import org.apache.poi.ss.usermodel.{CellType, DataFormatter, Row}

/**
  * Generic iterator over rows of data, represented as an array of strings
  */

sealed trait RowIterator extends Iterator[Array[String]]

/**
  * CSV row iterator
  *
  * @param iterator Univocity CSV ResultIterator
  */

class CsvRowIterator(iterator: ResultIterator[Array[String], ParsingContext]) extends RowIterator {

  override def hasNext: Boolean = iterator.hasNext

  override def next(): Array[String] = iterator.next
}

/**
  * Excel row iterator
  *
  * @param iterator an iterator of Apache POI rows
  */

class ExcelRowIterator(iterator: java.util.Iterator[Row], colDim: Int) extends RowIterator {

  private val formatter = new DataFormatter()

  override def hasNext: Boolean = iterator.hasNext

  override def next(): Array[String] = {

    val row = iterator.next()
    var j = 0
    val result = Array.fill[String](colDim)("")
    while (j < colDim) {
      val cell = row.getCell(j, MissingCellPolicy.CREATE_NULL_AS_BLANK)
      result(j) = cell.getCellType match {
        case CellType.BLANK | CellType.ERROR | CellType._NONE => ""
        case _ => formatter.formatCellValue(cell)
      }
      j += 1
    }
    result
  }
}
