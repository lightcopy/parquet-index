/*
 * Copyright 2016 Lightcopy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema._
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._

import org.apache.spark.sql.catalyst.util.DateTimeUtils

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class ParquetIndexReadSupportSuite extends UnitTestSuite {
  test("BufferRecordContainer - init") {
    val container = new BufferRecordContainer()
    container.toString should be ("BufferRecordContainer(buffer=null)")
    container.init(numFields = 2)
    container.toString should be ("BufferRecordContainer(buffer=[null, null])")
  }

  test("BufferRecordContainer - clear buffer with init") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.setString(0, "str")
    container.init(numFields = 1)
    container.toString should be ("BufferRecordContainer(buffer=[null])")
  }

  test("BufferRecordContainer - reset empty buffer with different numFields") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.toString should be ("BufferRecordContainer(buffer=[null])")
    container.init(numFields = 2)
    container.toString should be ("BufferRecordContainer(buffer=[null, null])")
  }

  test("BufferRecordContainer - reset filled buffer with different numFields") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.setInt(0, 5)
    container.toString should be ("BufferRecordContainer(buffer=[5])")
    container.init(numFields = 2)
    container.toString should be ("BufferRecordContainer(buffer=[null, null])")
  }

  test("BufferRecordContainer - reset filled buffer with same numFields") {
    val container = new BufferRecordContainer()
    container.init(numFields = 2)
    container.setInt(0, 5)
    container.setString(1, "a")
    container.toString should be ("BufferRecordContainer(buffer=[5, a])")
    container.init(numFields = 2)
    container.toString should be ("BufferRecordContainer(buffer=[null, null])")
  }

  test("BufferRecordContainer - set Parquet Binary type as Timestamp") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.setParquetBinary(0, new PrimitiveType(REQUIRED, INT96, "a"),
      Binary.fromConstantByteArray(new Array[Byte](12), 0, 12))
    container.getByIndex(0).isInstanceOf[java.sql.Timestamp] should be (true)
  }

  test("BufferRecordContainer - set Parquet Binary type as String") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.setParquetBinary(0, new PrimitiveType(REQUIRED, BINARY, "a"),
      Binary.fromString("a"))
    container.getByIndex(0).isInstanceOf[String] should be (true)
  }

  test("BufferRecordContainer - set Parquet Integer type as Date") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.setParquetInteger(0, new PrimitiveType(REQUIRED, INT32, "a", DATE), 1)
    container.getByIndex(0).isInstanceOf[java.sql.Date] should be (true)
  }

  test("BufferRecordContainer - fail to parse Integer with unsupported Primitive Type") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    val err = intercept[IllegalArgumentException] {
      container.setParquetInteger(0, new PrimitiveType(REQUIRED, INT64, "a"), 1)
    }
    assert(err.getMessage.contains(
      "Field required int64 a with value 1 at position 0 cannot be parsed as Parquet Integer"))
  }

  test("BufferRecordContainer - set Parquet Integer type as Int") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.setParquetInteger(0, new PrimitiveType(REQUIRED, INT32, "a"), 1)
    container.getByIndex(0).isInstanceOf[Int] should be (true)
  }

  test("BufferRecordContainer - parse java.sql.Date value") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.setParquetInteger(0, new PrimitiveType(REQUIRED, INT32, "a", DATE), 1)
    container.getByIndex(0) should be (DateTimeUtils.toJavaDate(1))
  }

  test("BufferRecordContainer - fail to parse byte array as java.sql.Timestamp") {
    val arr = new Array[Byte](14)
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    var err = intercept[AssertionError] {
      val bin = Binary.fromConstantByteArray(arr, 0, 11)
      container.setParquetBinary(0, new PrimitiveType(REQUIRED, INT96, "a"), bin)
    }
    assert(err.getMessage.contains(
      "Timestamps (with nanoseconds) are expected to be stored in 12-byte long binaries"))

    err = intercept[AssertionError] {
      val bin = Binary.fromConstantByteArray(arr, 0, 13)
      container.setParquetBinary(0, new PrimitiveType(REQUIRED, INT96, "a"), bin)
    }
  }

  test("BufferRecordContainer - parse byte array as java.sql.Timestamp") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    // same as 110000000000L or 1970-01-02 18:33:20.0
    val arr = Array[Byte](0, -32, -99, -51, 118, 21, 0, 0, -115, 61, 37, 0)
    val bin = Binary.fromConstantByteArray(arr, 0, arr.length)
    container.setParquetBinary(0, new PrimitiveType(REQUIRED, INT96, "a"), bin)
    container.getByIndex(0) should be (DateTimeUtils.toJavaTimestamp(110000000000L))
  }

  test("BufferRecordContainer - set all ordinals") {
    val container = new BufferRecordContainer()
    container.init(numFields = 7)
    container.setString(0, "str")
    container.setBoolean(1, true)
    container.setDouble(2, 0.2)
    container.setInt(3, 5)
    container.setLong(4, 7L)
    container.setDate(5, new java.sql.Date(1L))
    container.setTimestamp(6, new java.sql.Timestamp(1L))

    assert(container.getByIndex(0) === "str")
    assert(container.getByIndex(1) === true)
    assert(container.getByIndex(2) === 0.2)
    assert(container.getByIndex(3) === 5)
    assert(container.getByIndex(4) === 7L)
    assert(container.getByIndex(5) === new java.sql.Date(1L))
    assert(container.getByIndex(6) === new java.sql.Timestamp(1L))
  }

  test("BufferRecordContainer - overwrite value for ordinal") {
    val container = new BufferRecordContainer()
    container.init(numFields = 1)
    container.setString(0, "str")
    container.setInt(0, 123)

    assert(container.getByIndex(0) === 123)
  }

  test("ParquetIndexPrimitiveConverter - add fields") {
    val container = new BufferRecordContainer()
    container.init(numFields = 5)
    val c1 = new ParquetIndexPrimitiveConverter(0,
      new PrimitiveType(REQUIRED, BINARY, "a"), container)
    val c2 = new ParquetIndexPrimitiveConverter(1,
      new PrimitiveType(REQUIRED, BOOLEAN, "a"), container)
    val c3 = new ParquetIndexPrimitiveConverter(2,
      new PrimitiveType(REQUIRED, DOUBLE, "a"), container)
    val c4 = new ParquetIndexPrimitiveConverter(3,
      new PrimitiveType(REQUIRED, INT32, "a"), container)
    val c5 = new ParquetIndexPrimitiveConverter(4,
      new PrimitiveType(REQUIRED, INT64, "a"), container)

    c1.addBinary(Binary.fromString("abc"))
    c2.addBoolean(true)
    c3.addDouble(0.3)
    c4.addInt(123)
    c5.addLong(7L)

    assert(container.getByIndex(0) === "abc")
    assert(container.getByIndex(1) === true)
    assert(container.getByIndex(2) === 0.3)
    assert(container.getByIndex(3) === 123)
    assert(container.getByIndex(4) === 7L)
  }

  test("ParquetIndexGroupConverter - prepare converters") {
    val container = new BufferRecordContainer()
    val schema = new MessageType("root1",
      new PrimitiveType(REQUIRED, INT64, "a"),
      new PrimitiveType(OPTIONAL, BINARY, "b")).asGroupType
    val converter = new ParquetIndexGroupConverter(container, schema)
    assert(converter.getConverter(0) !== null)
    assert(converter.getConverter(1) !== null)
    intercept[ArrayIndexOutOfBoundsException] { converter.getConverter(-1) }
    intercept[ArrayIndexOutOfBoundsException] { converter.getConverter(2) }
  }

  test("ParquetIndexGroupConverter - fail if type is not primitive") {
    val container = new BufferRecordContainer()
    val schema = new MessageType("root1",
      new PrimitiveType(REQUIRED, INT64, "a"),
      new PrimitiveType(REQUIRED, BINARY, "b"),
      new GroupType(REQUIRED, "g1",
        new PrimitiveType(OPTIONAL, BINARY, "a"))).asGroupType
    val err = intercept[AssertionError] {
      new ParquetIndexGroupConverter(container, schema)
    }
    assert(err.getMessage.contains("Only primitive types are supported, found schema"))
  }
}
