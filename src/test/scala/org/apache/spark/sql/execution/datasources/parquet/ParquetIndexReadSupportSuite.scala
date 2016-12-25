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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class ParquetIndexReadSupportSuite extends UnitTestSuite {
  test("MapContainer - init") {
    val container = new MapContainer()
    container.init()
    container.toString should be ("MapContainer(buffer={})")
  }

  test("MapContainer - clear map with init") {
    val container = new MapContainer()
    container.init()
    container.setString(0, "str")
    container.init()
    container.toString should be ("MapContainer(buffer={})")
  }

  test("MapContainer - set all ordinals") {
    val container = new MapContainer()
    container.init()
    container.setString(0, "str")
    container.setBoolean(1, true)
    container.setDouble(2, 0.2)
    container.setInt(3, 5)
    container.setLong(4, 7L)

    assert(container.getByIndex(0) === "str")
    assert(container.getByIndex(1) === true)
    assert(container.getByIndex(2) === 0.2)
    assert(container.getByIndex(3) === 5)
    assert(container.getByIndex(4) === 7L)
  }

  test("MapContainer - overwrite value for ordinal") {
    val container = new MapContainer()
    container.init()
    container.setString(0, "str")
    container.setInt(0, 123)

    assert(container.getByIndex(0) === 123)
  }

  test("ParquetIndexPrimitiveConverter - add fields") {
    val container = new MapContainer()
    container.init()
    val c1 = new ParquetIndexPrimitiveConverter(0, container)
    val c2 = new ParquetIndexPrimitiveConverter(1, container)
    val c3 = new ParquetIndexPrimitiveConverter(2, container)
    val c4 = new ParquetIndexPrimitiveConverter(3, container)
    val c5 = new ParquetIndexPrimitiveConverter(4, container)

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
    val container = new MapContainer()
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
    val container = new MapContainer()
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
