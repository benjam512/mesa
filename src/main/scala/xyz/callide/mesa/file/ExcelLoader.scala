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

import java.io.{File, FileInputStream}

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import xyz.callide.mesa.data.DataSet
import xyz.callide.mesa.data.conversion.ConversionSet
import scala.collection.JavaConverters._

/**
  * Loads data sets from an Excel file
  */

object ExcelLoader extends FileLoader {

  /**
    * Reads from the provided file path
    *
    * @param path the file path
    * @param sheet the sheet number in the workbook, indexed at zero
    * @param header denotes whether or not the Excel file contains a header row
    * @param set conversion set
    * @return data set
    */

  def readFromFile(path: String,
                   sheet: Int = 0,
                   header: Boolean = true)(implicit set: ConversionSet): DataSet = {

    val stream = new FileInputStream(new File(path))
    val workbook = new XSSFWorkbook(stream)
    val colDim = workbook.getSheetAt(sheet).iterator.asScala.map(row => row.getLastCellNum).max.toInt
    read(new ExcelRowIterator(workbook.getSheetAt(sheet).iterator, colDim), header)
  }
}
