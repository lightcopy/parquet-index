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

import java.io.IOException

import org.apache.spark.sql.execution.datasources.TestMetastore
import org.apache.spark.sql.types.StructType

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class ParquetMetastoreSupportSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("identifier") {
    val support = new ParquetMetastoreSupport()
    support.identifier should be ("parquet")
  }

  test("backed file format") {
    val support = new ParquetMetastoreSupport()
    support.fileFormat.isInstanceOf[ParquetFileFormat]
  }

  test("fail if table metadata does not exist") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val support = new ParquetMetastoreSupport()
      val err = intercept[IOException] {
        support.loadIndex(metastore, fs.getFileStatus(dir))
      }
      assert(err.getMessage.contains("table metadata does not exist"))
    }
  }

  test("fail if there is a deserialization error") {
    withTempDir { dir =>
      touch(dir.toString / ParquetMetastoreSupport.TABLE_METADATA)
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val support = new ParquetMetastoreSupport()
      val err = intercept[IOException] {
        support.loadIndex(metastore, fs.getFileStatus(dir))
      }
      assert(err.getMessage.contains("Failed to deserialize object"))
    }
  }

  test("fail to create if partitions are empty") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val support = new ParquetMetastoreSupport()
      val status = fs.getFileStatus(dir)
      val err = intercept[IllegalArgumentException] {
        support.createIndex(metastore, status, status, false, null, Seq.empty, Seq.empty)
      }
      assert(err.getMessage.contains("Empty partitions provided"))
    }
  }

  test("ParquetMetastoreSupport does not support append mode") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val support = new ParquetMetastoreSupport()
      val status = fs.getFileStatus(dir)
      val err = intercept[UnsupportedOperationException] {
        support.createIndex(metastore, status, status, true, null, Seq(null), Seq.empty)
      }
      assert(err.getMessage.contains("does not support append to existing index"))
    }
  }

  test("ParquetMetastoreSupport - inferSchema, return empty struct if no statuses provided") {
    ParquetMetastoreSupport.inferSchema(spark, Array.empty) should be (StructType(Nil))
  }
}
