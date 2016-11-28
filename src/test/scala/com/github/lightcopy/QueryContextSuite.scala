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

package com.github.lightcopy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit

import com.github.lightcopy.index.Index
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

/** Test suite for [[QueryContext]] and [[DataFrameIndexBuilder]] */
class QueryContextSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("implicit query method for SQLContext") {
    import com.github.lightcopy.implicits._
    val builder = spark.sqlContext.index
    builder.isInstanceOf[DataFrameIndexBuilder] should be (true)
    // internal catalog is hard-coded for now
    builder.catalog.isInstanceOf[InternalCatalog] should be (true)
  }

  test("init index builder") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    val spec = builder.makeIndexSpec()
    spec.source should be (null)
    spec.path should be (None)
    spec.mode should be (SaveMode.ErrorIfExists)
    spec.options.isEmpty should be (true)
  }

  test("set source") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.format("test")
    val spec = builder.makeIndexSpec()
    spec.source should be ("test")
  }

  test("set null path") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.path(null)
    val spec = builder.makeIndexSpec()
    spec.path should be (None)
  }

  test("set non-null path") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.path("path")
    val spec = builder.makeIndexSpec()
    spec.path should be (Some("path"))
  }

  test("set single option") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.option("key", "value")
    val spec = builder.makeIndexSpec()
    spec.options.get("key") should be (Some("value"))
  }

  test("set multiple options") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.options(Map("key1" -> "1", "key2" -> "2")).option("key3", "3")
    val spec = builder.makeIndexSpec()
    spec.options.get("key1") should be (Some("1"))
    spec.options.get("key2") should be (Some("2"))
    spec.options.get("key3") should be (Some("3"))
  }

  test("set parquet shortcut") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.parquet("path").option("key", "value")
    val spec = builder.makeIndexSpec()
    spec.source should be ("parquet")
    spec.path should be (Some("path"))
    spec.options.get("key") should be (Some("value"))
  }

  test("set mode") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.mode(SaveMode.Overwrite)
    val spec = builder.makeIndexSpec()
    spec.mode should be (SaveMode.Overwrite)
  }

  test("set correct overwrite mode as string") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.mode("overwrite")
    val spec = builder.makeIndexSpec()
    spec.mode should be (SaveMode.Overwrite)
  }

  test("set correct append mode as string") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.mode("append")
    val spec = builder.makeIndexSpec()
    spec.mode should be (SaveMode.Append)
  }

  test("set correct ignore mode as string") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.mode("ignore")
    val spec = builder.makeIndexSpec()
    spec.mode should be (SaveMode.Ignore)
  }

  test("set correct error mode as string") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    builder.mode("error")
    val spec = builder.makeIndexSpec()
    spec.mode should be (SaveMode.ErrorIfExists)
  }

  test("fail if string mode is incorrect") {
    val builder = new DataFrameIndexBuilder(new SimpleCatalog())
    var err = intercept[IllegalArgumentException] {
      builder.mode(null.asInstanceOf[String])
    }
    err.getMessage.contains("Unknown save mode") should be (true)

    err = intercept[IllegalArgumentException] {
      builder.mode("test")
    }
    err.getMessage.contains("Unknown save mode") should be (true)

    err = intercept[IllegalArgumentException] {
      builder.mode("Append")
    }
    err.getMessage.contains("Unknown save mode") should be (true)
  }

  test("create index") {
    var called = false
    val builder = new DataFrameIndexBuilder(new SimpleCatalog() {
      override def createIndex(indexSpec: IndexSpec, columns: Seq[Column]): Unit = {
        columns.length should be (2)
        indexSpec.source should be ("parquet")
        indexSpec.path should be (Some("path"))
        indexSpec.mode should be (SaveMode.Append)
        called = true
      }
    })
    builder.mode("append").parquet("path").create(lit(1), lit("str"))
    called should be (true)
  }

  test("drop index") {
    var called = false
    val builder = new DataFrameIndexBuilder(new SimpleCatalog() {
      override def dropIndex(indexSpec: IndexSpec): Unit = {
        indexSpec.source should be ("parquet")
        indexSpec.path should be (Some("path"))
        called = true
      }
    })
    builder.parquet("path").drop()
    called should be (true)
  }

  test("query index") {
    var called = false
    val filter = lit(1).between(lit(0), lit(2))
    val builder = new DataFrameIndexBuilder(new SimpleCatalog() {
      override def queryIndex(indexSpec: IndexSpec, condition: Column): DataFrame = {
        indexSpec.source should be ("parquet")
        indexSpec.path should be (Some("path"))
        condition should be (filter)
        called = true
        null
      }
    })
    val df = builder.parquet("path").filter(filter)
    called should be (true)
    df should be (null)
  }
}
