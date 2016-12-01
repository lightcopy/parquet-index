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

package com.github.lightcopy.index

import org.apache.spark.sql.types._

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Tests for converting [[Metadata]] and [[SerDe]] */
class MetadataSuite extends UnitTestSuite {
  test("metadata - toString") {
    // should serialize object when printing string representation
    val schema = StructType(StructField("str", LongType) :: Nil)
    val metadata = Metadata("source", None, schema, Map("key" -> "value"))
    metadata.toString.contains("Metadata") should be (true)
    metadata.toString.contains(schema.json) should be (true)
  }

  test("serde empty metadata") {
    val schema = StructType(StructField("id", LongType) :: Nil)
    val metadata = Metadata("source", None, schema, Map.empty)

    val json = SerDe.serialize(metadata)
    val res = SerDe.deserialize(json)
    json.nonEmpty should be (true)
    res should be (metadata)
  }

  test("serde simple metadata with plain schema") {
    val schema = StructType(
      StructField("id", LongType) ::
      StructField("num", IntegerType) ::
      StructField("flag", BooleanType) ::
      StructField("str", StringType) :: Nil)
    val metadata = Metadata("source", Some("/path"), schema, Map("key" -> "value"))

    val json = SerDe.serialize(metadata)
    val res = SerDe.deserialize(json)
    json.nonEmpty should be (true)
    res should be (metadata)
  }

  test("serde metadata with nested type and complex map") {
    val schema = StructType(
      StructField("id", LongType, false) ::
      StructField("num", IntegerType, false) ::
      StructField("flag", BooleanType, false) ::
      StructField("arr", ArrayType(StringType), true) ::
      StructField("str", StringType, true) :: Nil)
    val metadata = Metadata("source", Some("/path"), schema,
      Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3"))

    val json = SerDe.serialize(metadata)
    val res = SerDe.deserialize(json)
    json.nonEmpty should be (true)
    res should be (metadata)
  }

  test("serde metadata with nested struct type and map") {
    val schema = StructType(
      StructField("id", LongType, false) ::
      StructField("num", IntegerType, false) ::
      StructField("flag", BooleanType, false) ::
      StructField("arr", StructType(
        StructField("nested1", LongType) ::
        StructField("nested2", BooleanType) ::
        StructField("nested3", StringType) :: Nil), true) ::
      StructField("str", StringType, true) :: Nil)
    val metadata = Metadata("source", Some("/path"), schema, Map("key" -> "value"))

    val json = SerDe.serialize(metadata)
    println(json)
    val res = SerDe.deserialize(json)
    json.nonEmpty should be (true)
    res should be (metadata)
  }
}
