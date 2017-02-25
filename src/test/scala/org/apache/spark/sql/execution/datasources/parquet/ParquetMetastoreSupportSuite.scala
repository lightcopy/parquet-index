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

import org.apache.spark.sql.execution.datasources.{TestMetastore, SourceLocationSpec}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.IndexConf._
import org.apache.spark.sql.types._

import com.github.lightcopy.implicits._
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class ParquetMetastoreSupportSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  // Reset SparkSession for every test, because Metastore caches instance per session, and we
  // do not reset options per metastore configuration.
  before {
    startSparkSession()
  }

  after {
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
      val metastore = testMetastore(spark, dir / "test_metastore")
      val support = new ParquetMetastoreSupport()
      val err = intercept[IOException] {
        support.loadIndex(metastore, fs.getFileStatus(dir))
      }
      assert(err.getMessage.contains("table metadata does not exist"))
    }
  }

  test("fail if there is a deserialization error") {
    withTempDir { dir =>
      touch(dir / ParquetMetastoreSupport.TABLE_METADATA)
      val metastore = testMetastore(spark, dir / "test_metastore")
      val support = new ParquetMetastoreSupport()
      val err = intercept[IOException] {
        support.loadIndex(metastore, fs.getFileStatus(dir))
      }
      assert(err.getMessage.contains("Failed to deserialize object"))
    }
  }

  test("fail to create if partitions are empty") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
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
      val metastore = testMetastore(spark, dir / "test_metastore")
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

  test("ParquetMetastoreSupport - inferSchema, merge when only fileSchema is available") {
    val fileSchema = """
      | message spark_schema {
      |   required int32 id;
      |   required binary str (UTF8);
      | }""".stripMargin
    val statuses = Array(
      ParquetFileStatus(null, fileSchema, Array.empty, None),
      ParquetFileStatus(null, fileSchema, Array.empty, None))

    ParquetMetastoreSupport.inferSchema(spark, statuses) should be (
      StructType(Nil).
        add("id", IntegerType, false, Metadata.empty).
        add("str", StringType, false, Metadata.empty))
  }

  test("ParquetMetastoreSupport - inferSchema, merge when sqlSchema is available") {
    // we do not need fileSchema to be set, since we parse sqlSchema
    val sqlSchema = """
    {
      "type" : "struct",
      "fields" : [ {
        "name" : "id",
        "type" : "integer",
        "nullable" : false,
        "metadata" : {
          "key" : "value1"
        }
      }, {
        "name" : "str",
        "type" : "string",
        "nullable" : false,
        "metadata" : {
          "key" : "value2"
        }
      } ]
    }
    """
    val statuses = Array(
      ParquetFileStatus(null, "", Array.empty, Some(sqlSchema)),
      ParquetFileStatus(null, "", Array.empty, Some(sqlSchema)))

    ParquetMetastoreSupport.inferSchema(spark, statuses) should be (
      StructType(Nil).
        add("id", IntegerType, false, new MetadataBuilder().putString("key", "value1").build()).
        add("str", StringType, false, new MetadataBuilder().putString("key", "value2").build()))
  }

  test("ParquetMetastoreSupport - fall back to fileSchema, if sqlSchema is corrupted") {
    val fileSchema = """
      | message spark_schema {
      |   required int32 id;
      |   required binary str (UTF8);
      | }""".stripMargin
    val sqlSchema = """abcdefg"""
    val statuses = Array(
      ParquetFileStatus(null, fileSchema, Array.empty, Some(sqlSchema)),
      ParquetFileStatus(null, fileSchema, Array.empty, Some(sqlSchema)))

    ParquetMetastoreSupport.inferSchema(spark, statuses) should be (
      StructType(Nil).
        add("id", IntegerType, false, Metadata.empty).
        add("str", StringType, false, Metadata.empty))
  }

  // One schema has metadata, the other one does not have metadata
  test("ParquetMetastoreSupport - merge schemas with different metadata 1") {
    val fileSchema = """
      | message spark_schema {
      |   required int32 id;
      |   required binary str (UTF8);
      | }""".stripMargin
    val sqlSchema = """
    {
      "type" : "struct",
      "fields" : [ {
        "name" : "id",
        "type" : "integer",
        "nullable" : false,
        "metadata" : {
          "key" : "value1"
        }
      }, {
        "name" : "str",
        "type" : "string",
        "nullable" : false,
        "metadata" : {
          "key" : "value2"
        }
      } ]
    }
    """
    val statuses = Array(
      ParquetFileStatus(null, fileSchema, Array.empty, None),
      ParquetFileStatus(null, fileSchema, Array.empty, Some(sqlSchema)))

    ParquetMetastoreSupport.inferSchema(spark, statuses) should be (
      StructType(Nil).
        add("id", IntegerType, false, new MetadataBuilder().putString("key", "value1").build()).
        add("str", StringType, false, new MetadataBuilder().putString("key", "value2").build()))
  }

  // Both schemas have metadata with different content
  test("ParquetMetastoreSupport - merge schemas with different metadata 2") {
    val sqlSchema1 = """
    {
      "type" : "struct",
      "fields" : [ {
        "name" : "id",
        "type" : "integer",
        "nullable" : false,
        "metadata" : {
          "key" : "value1"
        }
      }, {
        "name" : "str",
        "type" : "string",
        "nullable" : false,
        "metadata" : {
          "key" : "value2"
        }
      } ]
    }
    """

    val sqlSchema2 = """
    {
      "type" : "struct",
      "fields" : [ {
        "name" : "id",
        "type" : "integer",
        "nullable" : false,
        "metadata" : {
          "key2" : "value12"
        }
      }, {
        "name" : "str",
        "type" : "string",
        "nullable" : false,
        "metadata" : {
          "key2" : "value22"
        }
      } ]
    }
    """
    val statuses = Array(
      ParquetFileStatus(null, "", Array.empty, Some(sqlSchema1)),
      ParquetFileStatus(null, "", Array.empty, Some(sqlSchema2)))

    ParquetMetastoreSupport.inferSchema(spark, statuses) should be (
      StructType(Nil).
        add("id", IntegerType, false,
          new MetadataBuilder().putString("key", "value1").putString("key2", "value12").build()).
        add("str", StringType, false,
          new MetadataBuilder().putString("key", "value2").putString("key2", "value22").build()))
  }

  test("invoke loadIndex without eager loading") {
    withTempDir { dir =>
      val options = Map(
        METASTORE_LOCATION.key -> dir.toString / "test_metastore",
        PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
        PARQUET_FILTER_STATISTICS_EAGER_LOADING.key -> "false")
      withSQLConf(options) {
        val metastore = testMetastore(spark, options)
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "table")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "table")

        val support = new ParquetMetastoreSupport()
        val spec = SourceLocationSpec(support.identifier, dir / "table")
        val location = metastore.location(spec)

        val catalog = support.loadIndex(metastore, fs.getFileStatus(location)).
          asInstanceOf[ParquetIndexCatalog]
        catalog.indexMetadata.partitions.nonEmpty should be (true)
        catalog.indexMetadata.partitions.foreach { partition =>
          partition.files.nonEmpty should be (true)
          partition.files.foreach { status =>
            status.blocks.nonEmpty should be (true)
            status.blocks.foreach { block =>
              block.indexedColumns.values.foreach { metadata =>
                metadata.filter.isDefined should be (true)
                // none of filters should be loaded
                metadata.filter.get.isLoaded should be (false)
              }
            }
          }
        }
      }
    }
  }

  test("invoke loadIndex with eager loading") {
    withTempDir { dir =>
      val options = Map(
        METASTORE_LOCATION.key -> dir.toString / "test_metastore",
        PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
        PARQUET_FILTER_STATISTICS_EAGER_LOADING.key -> "true")
      withSQLConf(options) {
        val metastore = testMetastore(spark, options)
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "table")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "table")

        val support = new ParquetMetastoreSupport()
        val spec = SourceLocationSpec(support.identifier, dir / "table")
        val location = metastore.location(spec)

        val catalog = support.loadIndex(metastore, fs.getFileStatus(location)).
          asInstanceOf[ParquetIndexCatalog]
        catalog.indexMetadata.partitions.nonEmpty should be (true)
        catalog.indexMetadata.partitions.foreach { partition =>
          partition.files.nonEmpty should be (true)
          partition.files.foreach { status =>
            status.blocks.nonEmpty should be (true)
            status.blocks.foreach { block =>
              block.indexedColumns.values.foreach { metadata =>
                metadata.filter.isDefined should be (true)
                // all filters should be loaded
                metadata.filter.get.isLoaded should be (true)
              }
            }
          }
        }
      }
    }
  }

  // Test should not throw any exceptions if table index does not have any filter statistics
  test("invoke loadIndex with eager loading on index without filter statistics") {
    withTempDir { dir =>
      val options = Map(
        METASTORE_LOCATION.key -> dir.toString / "test_metastore",
        PARQUET_FILTER_STATISTICS_ENABLED.key -> "false",
        PARQUET_FILTER_STATISTICS_EAGER_LOADING.key -> "true")
      withSQLConf(options) {
        val metastore = testMetastore(spark, options)
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "table")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "table")

        val support = new ParquetMetastoreSupport()
        val spec = SourceLocationSpec(support.identifier, dir / "table")
        val location = metastore.location(spec)

        val catalog = support.loadIndex(metastore, fs.getFileStatus(location)).
          asInstanceOf[ParquetIndexCatalog]
        catalog.indexMetadata.partitions.nonEmpty should be (true)
        catalog.indexMetadata.partitions.foreach { partition =>
          partition.files.nonEmpty should be (true)
          partition.files.foreach { status =>
            status.blocks.nonEmpty should be (true)
            status.blocks.foreach { block =>
              block.indexedColumns.values.foreach { metadata =>
                // metadata should not contain any filter statistics
                metadata.filter.isDefined should be (false)
              }
            }
          }
        }
      }
    }
  }
}
