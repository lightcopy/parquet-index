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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.TestMetastore
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

/** Test in-filter that returns true, if value in provided sequence */
private[parquet] case class TestInFilter(values: Seq[Any]) extends ParquetColumnFilter {
  override def init(conf: Configuration): Unit = { }
  override def isSet(): Boolean = true
  override def  destroy(): Unit = { }
  override def mightContain(value: Any): Boolean = values.contains(value)
}

private[parquet] case class TestUnsupportedFilter() extends Filter

class ParquetIndexCatalogSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
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

  test("foldFilter - return trivial when EqualTo attribute is not indexed column") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(ParquetBlockMetadata(123, Map.empty))
    ParquetIndexCatalog.foldFilter(EqualTo("a", 1), conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - discard EqualTo when value is not in statistics") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexCatalog.foldFilter(EqualTo("a", 1), conf, blocks) should be (Trivial(false))
  }

  test("foldFilter - accept EqualTo when value is in statistics and no filter") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexCatalog.foldFilter(EqualTo("a", 3), conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - discard EqualTo when value is in statistics, but rejected by filter") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0),
            Some(TestInFilter(Nil)))
        )
      )
    )
    ParquetIndexCatalog.foldFilter(EqualTo("a", 3), conf, blocks) should be (Trivial(false))
  }

  test("foldFilter - accept EqualTo when value is in statistics, and in filter") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0),
            Some(TestInFilter(3 :: Nil)))
        )
      )
    )
    ParquetIndexCatalog.foldFilter(EqualTo("a", 3), conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - reduce all blocks results using Or") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      ),
      ParquetBlockMetadata(123, Map.empty)
    )
    // should return true, because first filter returns Trivial(false), second filter returns
    // Trivial(true), and result is Or(Trivial(true), Trivial(false))
    ParquetIndexCatalog.foldFilter(EqualTo("a", 1), conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - return true for unsupported filter") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = TestUnsupportedFilter()
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - And(true, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(true), Trivial(true))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - And(true, false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(true), Trivial(false))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(false))
  }

  test("foldFilter - And(false, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(false), Trivial(true))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(false))
  }

  test("foldFilter - And(false, false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(false), Trivial(false))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(false))
  }

  test("foldFilter - Or(true, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(true), Trivial(true))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - Or(true, false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(true), Trivial(false))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - Or(false, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(false), Trivial(true))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - Or(null, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(null, Trivial(true))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(true))
  }

  test("foldFilter - Or(false, false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(false), Trivial(false))
    ParquetIndexCatalog.foldFilter(filter, conf, blocks) should be (Trivial(false))
  }
}
