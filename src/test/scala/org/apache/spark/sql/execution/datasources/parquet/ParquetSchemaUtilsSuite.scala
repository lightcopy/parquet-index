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

import org.apache.parquet.schema.MessageTypeParser

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

  test("topLevelUniqueColumns - extract primitive columns for unique fields") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required boolean flag;
      |   required int64 id;
      |   required int32 index;
      |   required binary str (UTF8);
      | }
      """.stripMargin)
    ParquetSchemaUtils.topLevelUniqueColumns(schema) should be (
      ("flag", 0) :: ("id", 1) :: ("index", 2) :: ("str", 3) :: Nil)
  }

  test("topLevelUniqueColumns - extract mixed columns for unique fields") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required int64 id;
      |   required binary str (UTF8);
      |   optional group b {
      |     optional binary _1 (UTF8);
      |     required boolean _2;
      |   }
      |   optional group c (LIST) {
      |     repeated group list {
      |       required int32 element;
      |     }
      |   }
      | }
      """.stripMargin)
    ParquetSchemaUtils.topLevelUniqueColumns(schema) should be (
      ("id", 0) :: ("str", 1) :: ("b", 2) :: ("c", 3) :: Nil)
  }

  test("topLevelUniqueColumns - empty schema") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema { }
      """.stripMargin)
    ParquetSchemaUtils.topLevelUniqueColumns(schema) should be (Nil)
  }

  test("topLevelUniqueColumns - duplicate column names") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required int64 id;
      |   required int32 id;
      |   required binary str (UTF8);
      | }
      """.stripMargin)
    val err = intercept[IllegalArgumentException] {
      ParquetSchemaUtils.topLevelUniqueColumns(schema)
    }
    assert(err.getMessage.contains("[required int32 id] with duplicate column name id"))
  }

  test("merge - two identical schemas") {
    val schema1 = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "b").build)
      .add("col2", StringType, true, new MetadataBuilder().putString("c", "d").build)
    val schema2 = schema1

    ParquetSchemaUtils.merge(schema1, schema2) should be (schema1)
  }

  test("merge - two different schemas") {
    val schema1 = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
    val schema2 = StructType(Nil)
      .add("col2", StringType, true, new MetadataBuilder().putString("b", "1").build)

    val result = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, true, new MetadataBuilder().putString("b", "1").build)

    ParquetSchemaUtils.merge(schema1, schema2) should be (result)
  }

  test("merge - two interleaved schemas") {
    val schema1 = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, false, new MetadataBuilder().putString("b", "1").build)
    val schema2 = StructType(Nil)
      .add("col2", StringType, true, new MetadataBuilder().putString("c", "1").build)

    val result = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, true,
        new MetadataBuilder().putString("b", "1").putString("c", "1").build)

    ParquetSchemaUtils.merge(schema1, schema2) should be (result)
  }

  test("merge - two interleaved schemas with merged metadata for the same key") {
    val schema1 = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, false, new MetadataBuilder().putString("b", "1").build)
    val schema2 = StructType(Nil)
      .add("col2", StringType, true, new MetadataBuilder().putString("b", "2").build)

    val result = StructType(Nil)
      .add("col1", StringType, false, new MetadataBuilder().putString("a", "1").build)
      .add("col2", StringType, true, new MetadataBuilder().putString("b", "2").build)

    ParquetSchemaUtils.merge(schema1, schema2) should be (result)
  }
}
