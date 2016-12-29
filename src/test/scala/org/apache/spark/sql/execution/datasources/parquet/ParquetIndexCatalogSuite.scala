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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.github.lightcopy.util.SerializableFileStatus
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class ParquetIndexCatalogSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  private def testSerDeStatus: SerializableFileStatus = {
    SerializableFileStatus(
      path = "path",
      length = 1L,
      isDir = false,
      blockReplication = 1,
      blockSize = 128L,
      modificationTime = 1L,
      accessTime = 1L,
      blockLocations = Array.empty)
  }

  test("fail if index metadata is null") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val err = intercept[IllegalArgumentException] {
        new ParquetIndexCatalog(metastore, null)
      }
      assert(err.getMessage.contains("Parquet index metadata is null"))
    }
  }

  // Test invokes all overwritten methods for catalog to ensure that we return expected results
  test("initialize parquet index catalog") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata(
        "tablePath",
        StructType(StructField("a", LongType) :: Nil),
        StructType(StructField("a", LongType) :: Nil),
        PartitionSpec(StructType(Nil), Seq.empty),
        Seq(
          ParquetPartition(InternalRow.empty, Seq(
            ParquetFileStatus(
              status = testSerDeStatus,
              fileSchema = "schema",
              blocks = Array.empty
            )
          ))
        ))
      val catalog = new ParquetIndexCatalog(metastore, metadata)

      // check all metadata
      catalog.paths should be (Seq(new Path("tablePath")))
      catalog.allFiles should be (Seq(SerializableFileStatus.toFileStatus(testSerDeStatus)))
      catalog.dataSchema should be (StructType(StructField("a", LongType) :: Nil))
      catalog.indexSchema should be (StructType(StructField("a", LongType) :: Nil))
      catalog.partitionSpec should be (PartitionSpec(StructType(Nil), Seq.empty))
    }
  }

  test("prunePartitions - do not prune partitions for empty expressions") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndexCatalog(metastore, metadata)
      val spec = PartitionSpec(StructType(Nil), Seq(PartitionDirectory(InternalRow.empty, "path")))
      catalog.prunePartitions(Seq.empty, spec) should be (spec.partitions)
    }
  }

  test("prunePartitions - prune partitions for expressions") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndexCatalog(metastore, metadata)

      val spec = PartitionSpec(StructType(StructField("a", IntegerType) :: Nil), Seq(
        PartitionDirectory(InternalRow(1), new Path("path1")),
        PartitionDirectory(InternalRow(2), new Path("path2")),
        PartitionDirectory(InternalRow(3), new Path("path3"))
      ))

      val filter = expressions.Or(
        expressions.EqualTo(expressions.AttributeReference("a", IntegerType)(),
          expressions.Literal(1)),
        expressions.EqualTo(expressions.AttributeReference("a", IntegerType)(),
          expressions.Literal(3))
      )
      // remove "a=2" partition
      catalog.prunePartitions(filter :: Nil, spec) should be (
        PartitionDirectory(InternalRow(1), new Path("path1")) ::
        PartitionDirectory(InternalRow(3), new Path("path3")) :: Nil)
    }
  }

  test("pruneIndexedPartitions - fail if no indexed filters provided") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndexCatalog(metastore, metadata)
      val err = intercept[IllegalArgumentException] {
        catalog.pruneIndexedPartitions(Nil, Nil)
      }
      assert(err.getMessage.contains("Expected non-empty index filters"))
    }
  }

  test("pruneIndexedPartitions - fail if filter is not trivial") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndexCatalog(metastore, metadata) {
        override def resolveSupported(filter: Filter, status: ParquetFileStatus): Filter =
          TestUnsupportedFilter()
      }

      val err = intercept[RuntimeException] {
        catalog.pruneIndexedPartitions(Seq(TestUnsupportedFilter()), Seq(
          ParquetPartition(InternalRow.empty, Seq(
            ParquetFileStatus(null, null, Array.empty)
          ))
        ))
      }
      assert(err.getMessage.contains("Failed to resolve filter"))
    }
  }

  test("pruneIndexedPartitions - keep partition if true") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndexCatalog(metastore, metadata) {
        override def resolveSupported(filter: Filter, status: ParquetFileStatus): Filter =
          Trivial(true)
      }

      val partitions = Seq(
        ParquetPartition(InternalRow.empty, Seq(
          ParquetFileStatus(null, null, Array.empty)
        ))
      )
      catalog.pruneIndexedPartitions(Seq(Trivial(true)), partitions) should be (partitions)
    }
  }

  test("pruneIndexedPartitions - remove partition if false") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndexCatalog(metastore, metadata) {
        override def resolveSupported(filter: Filter, status: ParquetFileStatus): Filter =
          Trivial(false)
      }

      val partitions = Seq(
        ParquetPartition(InternalRow.empty, Seq(
          ParquetFileStatus(null, null, Array.empty)
        ))
      )
      catalog.pruneIndexedPartitions(Seq(Trivial(false)), partitions) should be (Nil)
    }
  }

  test("resolveSupported - return provided trivial filter") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val status = ParquetFileStatus(null, null, Array(null))
      val catalog = new ParquetIndexCatalog(metastore, metadata)
      catalog.resolveSupported(Trivial(false), status) should be (Trivial(false))
      catalog.resolveSupported(Trivial(true), status) should be (Trivial(true))
    }
  }

  test("resolveSupported - fail when no blocks provided") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir.toString / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val status = ParquetFileStatus(null, null, Array.empty)
      val catalog = new ParquetIndexCatalog(metastore, metadata)
      val err = intercept[IllegalArgumentException] {
        catalog.resolveSupported(EqualTo("a", 1), status)
      }
      assert(err.getMessage.contains(
        "Parquet file status has empty blocks, required at least one block metadata"))
    }
  }
}
