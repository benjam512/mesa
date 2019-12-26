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

package xyz.callide.mesa.sql

import java.sql.ResultSetMetaData

class MockResultSetMetaData() extends ResultSetMetaData {

  override def getColumnCount: Int = 7

  override def getColumnType(column: Int): Int = {

    column match {
      case 1 => 16
      case 2 => 8
      case 3 => 5
      case 4 => -5
      case 5 => 12
      case 6 => 91
      case 7 => 93
      case _ => throw new IllegalArgumentException("Invalid column: " + column)
    }
  }

  override def isAutoIncrement(i: Int): Boolean = false

  override def isCaseSensitive(i: Int): Boolean = false

  override def isSearchable(i: Int): Boolean = false

  override def isCurrency(i: Int): Boolean = false

  override def isNullable(i: Int): Int = 0

  override def isSigned(i: Int): Boolean = false

  override def getColumnDisplaySize(i: Int): Int = 0

  override def getColumnLabel(i: Int): String = throw new RuntimeException("unimplemented")

  override def getColumnName(i: Int): String = {

    i match {
      case 1 => "booleans"
      case 2 => "doubles"
      case 3 => "ints"
      case 4 => "longs"
      case 5 => "strings"
      case 6 => "dates"
      case 7 => "date-times"
    }
  }

  override def getSchemaName(i: Int): String = throw new RuntimeException("unimplemented")

  override def getPrecision(i: Int): Int = throw new RuntimeException("unimplemented")

  override def getScale(i: Int): Int = throw new RuntimeException("unimplemented")

  override def getTableName(i: Int): String = throw new RuntimeException("unimplemented")

  override def getCatalogName(i: Int): String = throw new RuntimeException("unimplemented")

  override def getColumnTypeName(i: Int): String = throw new RuntimeException("unimplemented")

  override def isReadOnly(i: Int): Boolean = throw new RuntimeException("unimplemented")

  override def isWritable(i: Int): Boolean = throw new RuntimeException("unimplemented")

  override def isDefinitelyWritable(i: Int): Boolean = throw new RuntimeException("unimplemented")

  override def getColumnClassName(i: Int): String = throw new RuntimeException("unimplemented")

  override def unwrap[T](aClass: Class[T]): T = throw new RuntimeException("unimplemented")

  override def isWrapperFor(aClass: Class[_]): Boolean = throw new RuntimeException("unimplemented")
}
