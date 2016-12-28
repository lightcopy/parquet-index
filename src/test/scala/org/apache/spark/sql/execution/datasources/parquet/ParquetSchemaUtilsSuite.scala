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

import org.apache.spark.sql.types._

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class ParquetSchemaUtilsSuite extends UnitTestSuite {
  test("validateStructType - empty schema") {
    val err = intercept[UnsupportedOperationException] {
      ParquetSchemaUtils.validateStructType(StructType(Nil))
    }
    assert(err.getMessage.contains("Empty schema StructType() is not supported"))
  }

  test("validateStructType - contains unsupported field") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", StringType) ::
      StructField("c", StructType(
        StructField("c1", IntegerType) :: Nil)
      ) :: Nil)
    val err = intercept[UnsupportedOperationException] {
      ParquetSchemaUtils.validateStructType(schema)
    }
    assert(err.getMessage.contains("Schema contains unsupported type, " +
      "field=StructField(c,StructType(StructField(c1,IntegerType,true)),true)"))
  }

  test("validateStructType - all fields are ok") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", StringType) ::
      StructField("c", LongType) :: Nil)
    ParquetSchemaUtils.validateStructType(schema)
  }

  test("pruneStructType - empty schema") {
    val schema = StructType(Nil)
    ParquetSchemaUtils.pruneStructType(schema) should be (schema)
  }

  test("pruneStructType - all supported types") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) ::
      StructField("c", StringType) :: Nil)
    ParquetSchemaUtils.pruneStructType(schema) should be (schema)
  }

  test("pruneStructType - mix of supported and not supported types") {
    val schema = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) ::
      StructField("c", StringType) ::
      StructField("d", BooleanType) ::
      StructField("e", ArrayType(LongType)) :: Nil)

    val expected = StructType(
      StructField("a", IntegerType) ::
      StructField("b", LongType) ::
      StructField("c", StringType) :: Nil)

    ParquetSchemaUtils.pruneStructType(schema) should be (expected)
  }

  test("pruneStructType - all not supported types") {
    val schema = StructType(
      StructField("a", ArrayType(IntegerType)) ::
      StructField("b", StructType(
        StructField("c", LongType) ::
        StructField("d", StringType) :: Nil)
      ) :: Nil)

    ParquetSchemaUtils.pruneStructType(schema) should be (StructType(Nil))
  }
}
