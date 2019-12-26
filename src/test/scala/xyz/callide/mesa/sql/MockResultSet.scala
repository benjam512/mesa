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

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql.{Blob, Clob, Date, NClob, Ref, ResultSet, ResultSetMetaData, RowId, SQLWarning, SQLXML, Statement, Time, Timestamp}
import java.time.{LocalDate, ZoneOffset}
import java.{sql, util}
import java.util.Calendar

class MockResultSet extends ResultSet {

  private var iter = -1
  private val maxIter = 5

  private val startLocalDate = LocalDate.of(2000, 1, 1)
  private val localDateVals = Array(1, 2, 3, 4, 5).map(i => startLocalDate.plusDays(i))
  private val localDateTimeVals = Array(1, 2, 3, 4, 5).map(i => startLocalDate.minusDays(i).atTime(12, 0))

  private val booleanVals = Array(true, false, false, true, true)
  private val doubleVals = Array(1.234, 2.345, 3.456, 4.567, 5.678)
  private val intVals = Array(2, 4, 6, 8, 10)
  private val longVals = Array(1, 3, 5, 7, 9)
  private val stringVals = Array("abc", "def", "ghi", "jkl", "mno")
  private val dateVals = localDateVals.map(ld => new Date(ld.atStartOfDay(ZoneOffset.UTC).toEpochSecond * 1000))
  private val timestampVals = localDateTimeVals.map(ldt => new Timestamp(ldt.toEpochSecond(ZoneOffset.UTC) * 1000))

  override def next(): Boolean = {

    iter += 1
    iter < maxIter
  }

  override def close(): Unit = throw new RuntimeException("unimplemented")

  override def wasNull(): Boolean = throw new RuntimeException("unimplemented")

  override def getString(i: Int): String = {

    i match {
      case 1 => booleanVals(iter).toString
      case 2 => doubleVals(iter).toString
      case 3 => intVals(iter).toString
      case 4 => longVals(iter).toString
      case 5 => stringVals(iter)
      case 6 => dateVals(iter).toString
      case 7 => timestampVals(iter).toString
    }
  }

  override def getBoolean(i: Int): Boolean = {

    i match {
      case 1 => booleanVals(iter)
      case _ => throw new RuntimeException("Not a boolean field")
    }
  }

  override def getByte(i: Int): Byte = throw new RuntimeException("unimplemented")

  override def getShort(i: Int): Short = throw new RuntimeException("unimplemented")

  override def getInt(i: Int): Int = {

    i match {
      case 3 => intVals(iter)
      case _ => throw new RuntimeException("Not an int field")
    }
  }

  override def getLong(i: Int): Long = {

    i match {
      case 4 => longVals(iter)
      case _ => throw new RuntimeException("Not a long field")
    }
  }

  override def getFloat(i: Int): Float = throw new RuntimeException("unimplemented")

  override def getDouble(i: Int): Double = {

    i match {
      case 2 => doubleVals(iter)
      case _ => throw new RuntimeException("Not a double field")
    }
  }

  override def getBigDecimal(i: Int, i1: Int): java.math.BigDecimal = throw new RuntimeException("unimplemented")

  override def getBytes(i: Int): Array[Byte] = throw new RuntimeException("unimplemented")

  override def getDate(i: Int): Date = {

    i match {
      case 6 => dateVals(iter)
      case _ => throw new RuntimeException("Not a date field")
    }
  }

  override def getTime(i: Int): Time = throw new RuntimeException("unimplemented")

  override def getTimestamp(i: Int): Timestamp = {

    i match {
      case 7 => timestampVals(iter)
      case _ => throw new RuntimeException("Not a timestamp field")
    }
  }

  override def getAsciiStream(i: Int): InputStream = throw new RuntimeException("unimplemented")

  override def getUnicodeStream(i: Int): InputStream = throw new RuntimeException("unimplemented")

  override def getBinaryStream(i: Int): InputStream = throw new RuntimeException("unimplemented")

  override def getString(s: String): String = throw new RuntimeException("unimplemented")

  override def getBoolean(s: String): Boolean = throw new RuntimeException("unimplemented")

  override def getByte(s: String): Byte = throw new RuntimeException("unimplemented")

  override def getShort(s: String): Short = throw new RuntimeException("unimplemented")

  override def getInt(s: String): Int = throw new RuntimeException("unimplemented")

  override def getLong(s: String): Long = throw new RuntimeException("unimplemented")

  override def getFloat(s: String): Float = throw new RuntimeException("unimplemented")

  override def getDouble(s: String): Double = throw new RuntimeException("unimplemented")

  override def getBigDecimal(s: String, i: Int): java.math.BigDecimal = throw new RuntimeException("unimplemented")

  override def getBytes(s: String): Array[Byte] = throw new RuntimeException("unimplemented")

  override def getDate(s: String): Date = throw new RuntimeException("unimplemented")

  override def getTime(s: String): Time = throw new RuntimeException("unimplemented")

  override def getTimestamp(s: String): Timestamp = throw new RuntimeException("unimplemented")

  override def getAsciiStream(s: String): InputStream = throw new RuntimeException("unimplemented")

  override def getUnicodeStream(s: String): InputStream = throw new RuntimeException("unimplemented")

  override def getBinaryStream(s: String): InputStream = throw new RuntimeException("unimplemented")

  override def getWarnings: SQLWarning = throw new RuntimeException("unimplemented")

  override def clearWarnings(): Unit = throw new RuntimeException("unimplemented")

  override def getCursorName: String = throw new RuntimeException("unimplemented")

  override def getMetaData: ResultSetMetaData = new MockResultSetMetaData()

  override def getObject(i: Int): AnyRef = throw new RuntimeException("unimplemented")

  override def getObject(s: String): AnyRef = throw new RuntimeException("unimplemented")

  override def findColumn(s: String): Int = throw new RuntimeException("unimplemented")

  override def getCharacterStream(i: Int): Reader = throw new RuntimeException("unimplemented")

  override def getCharacterStream(s: String): Reader = throw new RuntimeException("unimplemented")

  override def getBigDecimal(i: Int): java.math.BigDecimal = throw new RuntimeException("unimplemented")

  override def getBigDecimal(s: String): java.math.BigDecimal = throw new RuntimeException("unimplemented")

  override def isBeforeFirst: Boolean = throw new RuntimeException("unimplemented")

  override def isAfterLast: Boolean = throw new RuntimeException("unimplemented")

  override def isFirst: Boolean = throw new RuntimeException("unimplemented")

  override def isLast: Boolean = throw new RuntimeException("unimplemented")

  override def beforeFirst(): Unit = throw new RuntimeException("unimplemented")

  override def afterLast(): Unit = throw new RuntimeException("unimplemented")

  override def first(): Boolean = throw new RuntimeException("unimplemented")

  override def last(): Boolean = throw new RuntimeException("unimplemented")

  override def getRow: Int = throw new RuntimeException("unimplemented")

  override def absolute(i: Int): Boolean = throw new RuntimeException("unimplemented")

  override def relative(i: Int): Boolean = throw new RuntimeException("unimplemented")

  override def previous(): Boolean = throw new RuntimeException("unimplemented")

  override def setFetchDirection(i: Int): Unit = throw new RuntimeException("unimplemented")

  override def getFetchDirection: Int = throw new RuntimeException("unimplemented")

  override def setFetchSize(i: Int): Unit = throw new RuntimeException("unimplemented")

  override def getFetchSize: Int = throw new RuntimeException("unimplemented")

  override def getType: Int = throw new RuntimeException("unimplemented")

  override def getConcurrency: Int = throw new RuntimeException("unimplemented")

  override def rowUpdated(): Boolean = throw new RuntimeException("unimplemented")

  override def rowInserted(): Boolean = throw new RuntimeException("unimplemented")

  override def rowDeleted(): Boolean = throw new RuntimeException("unimplemented")

  override def updateNull(i: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateBoolean(i: Int, b: Boolean): Unit = throw new RuntimeException("unimplemented")

  override def updateByte(i: Int, b: Byte): Unit = throw new RuntimeException("unimplemented")

  override def updateShort(i: Int, i1: Short): Unit = throw new RuntimeException("unimplemented")

  override def updateInt(i: Int, i1: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateLong(i: Int, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateFloat(i: Int, v: Float): Unit = throw new RuntimeException("unimplemented")

  override def updateDouble(i: Int, v: Double): Unit = throw new RuntimeException("unimplemented")

  override def updateBigDecimal(i: Int, bigDecimal: java.math.BigDecimal): Unit = throw new RuntimeException("unimplemented")

  override def updateString(i: Int, s: String): Unit = throw new RuntimeException("unimplemented")

  override def updateBytes(i: Int, bytes: Array[Byte]): Unit = throw new RuntimeException("unimplemented")

  override def updateDate(i: Int, date: Date): Unit = throw new RuntimeException("unimplemented")

  override def updateTime(i: Int, time: Time): Unit = throw new RuntimeException("unimplemented")

  override def updateTimestamp(i: Int, timestamp: Timestamp): Unit = throw new RuntimeException("unimplemented")

  override def updateAsciiStream(i: Int, inputStream: InputStream, i1: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateBinaryStream(i: Int, inputStream: InputStream, i1: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateCharacterStream(i: Int, reader: Reader, i1: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateObject(i: Int, o: Any, i1: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateObject(i: Int, o: Any): Unit = throw new RuntimeException("unimplemented")

  override def updateNull(s: String): Unit = throw new RuntimeException("unimplemented")

  override def updateBoolean(s: String, b: Boolean): Unit = throw new RuntimeException("unimplemented")

  override def updateByte(s: String, b: Byte): Unit = throw new RuntimeException("unimplemented")

  override def updateShort(s: String, i: Short): Unit = throw new RuntimeException("unimplemented")

  override def updateInt(s: String, i: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateLong(s: String, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateFloat(s: String, v: Float): Unit = throw new RuntimeException("unimplemented")

  override def updateDouble(s: String, v: Double): Unit = throw new RuntimeException("unimplemented")

  override def updateBigDecimal(s: String, bigDecimal: java.math.BigDecimal): Unit = throw new RuntimeException("unimplemented")

  override def updateString(s: String, s1: String): Unit = throw new RuntimeException("unimplemented")

  override def updateBytes(s: String, bytes: Array[Byte]): Unit = throw new RuntimeException("unimplemented")

  override def updateDate(s: String, date: Date): Unit = throw new RuntimeException("unimplemented")

  override def updateTime(s: String, time: Time): Unit = throw new RuntimeException("unimplemented")

  override def updateTimestamp(s: String, timestamp: Timestamp): Unit = throw new RuntimeException("unimplemented")

  override def updateAsciiStream(s: String, inputStream: InputStream, i: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateBinaryStream(s: String, inputStream: InputStream, i: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateCharacterStream(s: String, reader: Reader, i: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateObject(s: String, o: Any, i: Int): Unit = throw new RuntimeException("unimplemented")

  override def updateObject(s: String, o: Any): Unit = throw new RuntimeException("unimplemented")

  override def insertRow(): Unit = throw new RuntimeException("unimplemented")

  override def updateRow(): Unit = throw new RuntimeException("unimplemented")

  override def deleteRow(): Unit = throw new RuntimeException("unimplemented")

  override def refreshRow(): Unit = throw new RuntimeException("unimplemented")

  override def cancelRowUpdates(): Unit = throw new RuntimeException("unimplemented")

  override def moveToInsertRow(): Unit = throw new RuntimeException("unimplemented")

  override def moveToCurrentRow(): Unit = throw new RuntimeException("unimplemented")

  override def getStatement: Statement = throw new RuntimeException("unimplemented")

  override def getObject(i: Int, map: util.Map[String, Class[_]]): AnyRef = throw new RuntimeException("unimplemented")

  override def getRef(i: Int): Ref = throw new RuntimeException("unimplemented")

  override def getBlob(i: Int): Blob = throw new RuntimeException("unimplemented")

  override def getClob(i: Int): Clob = throw new RuntimeException("unimplemented")

  override def getArray(i: Int): sql.Array = throw new RuntimeException("unimplemented")

  override def getObject(s: String, map: util.Map[String, Class[_]]): AnyRef = throw new RuntimeException("unimplemented")

  override def getRef(s: String): Ref = throw new RuntimeException("unimplemented")

  override def getBlob(s: String): Blob = throw new RuntimeException("unimplemented")

  override def getClob(s: String): Clob = throw new RuntimeException("unimplemented")

  override def getArray(s: String): sql.Array = throw new RuntimeException("unimplemented")

  override def getDate(i: Int, calendar: Calendar): Date = throw new RuntimeException("unimplemented")

  override def getDate(s: String, calendar: Calendar): Date = throw new RuntimeException("unimplemented")

  override def getTime(i: Int, calendar: Calendar): Time = throw new RuntimeException("unimplemented")

  override def getTime(s: String, calendar: Calendar): Time = throw new RuntimeException("unimplemented")

  override def getTimestamp(i: Int, calendar: Calendar): Timestamp = throw new RuntimeException("unimplemented")

  override def getTimestamp(s: String, calendar: Calendar): Timestamp = throw new RuntimeException("unimplemented")

  override def getURL(i: Int): URL = throw new RuntimeException("unimplemented")

  override def getURL(s: String): URL = throw new RuntimeException("unimplemented")

  override def updateRef(i: Int, ref: Ref): Unit = throw new RuntimeException("unimplemented")

  override def updateRef(s: String, ref: Ref): Unit = throw new RuntimeException("unimplemented")

  override def updateBlob(i: Int, blob: Blob): Unit = throw new RuntimeException("unimplemented")

  override def updateBlob(s: String, blob: Blob): Unit = throw new RuntimeException("unimplemented")

  override def updateClob(i: Int, clob: Clob): Unit = throw new RuntimeException("unimplemented")

  override def updateClob(s: String, clob: Clob): Unit = throw new RuntimeException("unimplemented")

  override def updateArray(i: Int, array: sql.Array): Unit = throw new RuntimeException("unimplemented")

  override def updateArray(s: String, array: sql.Array): Unit = throw new RuntimeException("unimplemented")

  override def getRowId(i: Int): RowId = throw new RuntimeException("unimplemented")

  override def getRowId(s: String): RowId = throw new RuntimeException("unimplemented")

  override def updateRowId(i: Int, rowId: RowId): Unit = throw new RuntimeException("unimplemented")

  override def updateRowId(s: String, rowId: RowId): Unit = throw new RuntimeException("unimplemented")

  override def getHoldability: Int = throw new RuntimeException("unimplemented")

  override def isClosed: Boolean = throw new RuntimeException("unimplemented")

  override def updateNString(i: Int, s: String): Unit = throw new RuntimeException("unimplemented")

  override def updateNString(s: String, s1: String): Unit = throw new RuntimeException("unimplemented")

  override def updateNClob(i: Int, nClob: NClob): Unit = throw new RuntimeException("unimplemented")

  override def updateNClob(s: String, nClob: NClob): Unit = throw new RuntimeException("unimplemented")

  override def getNClob(i: Int): NClob = throw new RuntimeException("unimplemented")

  override def getNClob(s: String): NClob = throw new RuntimeException("unimplemented")

  override def getSQLXML(i: Int): SQLXML = throw new RuntimeException("unimplemented")

  override def getSQLXML(s: String): SQLXML = throw new RuntimeException("unimplemented")

  override def updateSQLXML(i: Int, sqlxml: SQLXML): Unit = throw new RuntimeException("unimplemented")

  override def updateSQLXML(s: String, sqlxml: SQLXML): Unit = throw new RuntimeException("unimplemented")

  override def getNString(i: Int): String = throw new RuntimeException("unimplemented")

  override def getNString(s: String): String = throw new RuntimeException("unimplemented")

  override def getNCharacterStream(i: Int): Reader = throw new RuntimeException("unimplemented")

  override def getNCharacterStream(s: String): Reader = throw new RuntimeException("unimplemented")

  override def updateNCharacterStream(i: Int, reader: Reader, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateNCharacterStream(s: String, reader: Reader, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateAsciiStream(i: Int, inputStream: InputStream, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateBinaryStream(i: Int, inputStream: InputStream, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateCharacterStream(i: Int, reader: Reader, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateAsciiStream(s: String, inputStream: InputStream, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateBinaryStream(s: String, inputStream: InputStream, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateCharacterStream(s: String, reader: Reader, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateBlob(i: Int, inputStream: InputStream, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateBlob(s: String, inputStream: InputStream, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateClob(i: Int, reader: Reader, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateClob(s: String, reader: Reader, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateNClob(i: Int, reader: Reader, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateNClob(s: String, reader: Reader, l: Long): Unit = throw new RuntimeException("unimplemented")

  override def updateNCharacterStream(i: Int, reader: Reader): Unit = throw new RuntimeException("unimplemented")

  override def updateNCharacterStream(s: String, reader: Reader): Unit = throw new RuntimeException("unimplemented")

  override def updateAsciiStream(i: Int, inputStream: InputStream): Unit = throw new RuntimeException("unimplemented")

  override def updateBinaryStream(i: Int, inputStream: InputStream): Unit = throw new RuntimeException("unimplemented")

  override def updateCharacterStream(i: Int, reader: Reader): Unit = throw new RuntimeException("unimplemented")

  override def updateAsciiStream(s: String, inputStream: InputStream): Unit = throw new RuntimeException("unimplemented")

  override def updateBinaryStream(s: String, inputStream: InputStream): Unit = throw new RuntimeException("unimplemented")

  override def updateCharacterStream(s: String, reader: Reader): Unit = throw new RuntimeException("unimplemented")

  override def updateBlob(i: Int, inputStream: InputStream): Unit = throw new RuntimeException("unimplemented")

  override def updateBlob(s: String, inputStream: InputStream): Unit = throw new RuntimeException("unimplemented")

  override def updateClob(i: Int, reader: Reader): Unit = throw new RuntimeException("unimplemented")

  override def updateClob(s: String, reader: Reader): Unit = throw new RuntimeException("unimplemented")

  override def updateNClob(i: Int, reader: Reader): Unit = throw new RuntimeException("unimplemented")

  override def updateNClob(s: String, reader: Reader): Unit = throw new RuntimeException("unimplemented")

  override def getObject[T](i: Int, aClass: Class[T]): T = throw new RuntimeException("unimplemented")

  override def getObject[T](s: String, aClass: Class[T]): T = throw new RuntimeException("unimplemented")

  override def unwrap[T](aClass: Class[T]): T = throw new RuntimeException("unimplemented")

  override def isWrapperFor(aClass: Class[_]): Boolean = throw new RuntimeException("unimplemented")
}
