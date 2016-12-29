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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.util.sketch.BloomFilter

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class StatisticsSuite extends UnitTestSuite {
  // == Integer statistics ==
  test("ParquetIntStatistics - invalid statistics") {
    var err = intercept[IllegalArgumentException] {
      ParquetIntStatistics(1, -1, 10L)
    }
    assert(err.getMessage.contains("Min 1 is expected to be less than or equal to max -1"))

    err = intercept[IllegalArgumentException] {
      ParquetIntStatistics(10, 20, -1L)
    }
    assert(err.getMessage.contains("Number of nulls -1 is negative"))
  }

  test("ParquetIntStatistics - valid statistics") {
    val stats = ParquetIntStatistics(11, 14, 1298L)
    stats.getMin should be (11)
    stats.getMax should be (14)
    stats.getNumNulls should be (1298L)
    stats.hasNull should be (true)
    stats.toString should be ("ParquetIntStatistics(min=11, max=14, nulls=1298)")
  }

  test("ParquetIntStatistics - valid not-null statitics") {
    val stats = ParquetIntStatistics(11, 14, 0L)
    stats.getMin should be (11)
    stats.getMax should be (14)
    stats.getNumNulls should be (0L)
    stats.hasNull should be (false)
  }

  test("ParquetIntStatistics - contains") {
    val stats = ParquetIntStatistics(7, 128, 120L)
    // different contains checks
    stats.contains(Int.MinValue) should be (false)
    stats.contains(-1) should be (false)
    stats.contains(7) should be (true)
    stats.contains(96) should be (true)
    stats.contains(128) should be (true)
    stats.contains(Int.MaxValue) should be (false)
  }

  test("ParquetIntStatistics - contains nulls") {
    ParquetIntStatistics(7, 128, 0L).contains(null) should be (false)
    ParquetIntStatistics(7, 128, 1L).contains(null) should be (true)
  }

  test("ParquetIntStatistics - contains other types") {
    ParquetIntStatistics(7, 128, 200L).contains("str") should be (false)
    ParquetIntStatistics(7, 128, 200L).contains(Array(1, 2, 3)) should be (false)
    ParquetIntStatistics(7, 128, 200L).contains(true) should be (false)
    ParquetIntStatistics(7, 128, 200L).contains(false) should be (false)
  }

  // integer statistics - isLessThanMin

  test("ParquetIntStatistics - isLessThanMin") {
    ParquetIntStatistics(10, 20, 1L).isLessThanMin(9) should be (true)
    ParquetIntStatistics(10, 20, 1L).isLessThanMin(10) should be (false)
    ParquetIntStatistics(10, 20, 1L).isLessThanMin(20) should be (false)
  }

  test("ParquetIntStatistics - isLessThanMin, null") {
    ParquetIntStatistics(10, 20, 1L).isLessThanMin(null) should be (false)
    ParquetIntStatistics(10, 20, 0L).isLessThanMin(null) should be (false)
  }

  test("ParquetIntStatistics - isLessThanMin, other types") {
    ParquetIntStatistics(10, 20, 1L).isLessThanMin("abc") should be (false)
    ParquetIntStatistics(10, 20, 1L).isLessThanMin(Array(1, 2, 3)) should be (false)
  }

  // integer statistics - isEqualToMin

  test("ParquetIntStatistics - isEqualToMin") {
    ParquetIntStatistics(10, 20, 1L).isEqualToMin(10) should be (true)
    ParquetIntStatistics(10, 20, 1L).isEqualToMin(20) should be (false)
  }

  test("ParquetIntStatistics - isEqualToMin, null") {
    ParquetIntStatistics(10, 20, 1L).isEqualToMin(null) should be (false)
    ParquetIntStatistics(10, 20, 0L).isEqualToMin(null) should be (false)
  }

  test("ParquetIntStatistics - isEqualToMin, other types") {
    ParquetIntStatistics(10, 20, 1L).isEqualToMin("abc") should be (false)
    ParquetIntStatistics(10, 20, 1L).isEqualToMin(Array(1, 2, 3)) should be (false)
  }

  // integer statistics - isGreaterThanMax

  test("ParquetIntStatistics - isGreaterThanMax") {
    ParquetIntStatistics(10, 20, 1L).isGreaterThanMax(10) should be (false)
    ParquetIntStatistics(10, 20, 1L).isGreaterThanMax(20) should be (false)
    ParquetIntStatistics(10, 20, 1L).isGreaterThanMax(21) should be (true)
  }

  test("ParquetIntStatistics - isGreaterThanMax, null") {
    ParquetIntStatistics(10, 20, 1L).isGreaterThanMax(null) should be (false)
    ParquetIntStatistics(10, 20, 0L).isGreaterThanMax(null) should be (false)
  }

  test("ParquetIntStatistics - isGreaterThanMax, other types") {
    ParquetIntStatistics(10, 20, 1L).isGreaterThanMax("abc") should be (false)
    ParquetIntStatistics(10, 20, 1L).isGreaterThanMax(Array(1, 2, 3)) should be (false)
  }

  // integer statistics - isEqualToMax

  test("ParquetIntStatistics - isEqualToMax") {
    ParquetIntStatistics(10, 20, 1L).isEqualToMax(10) should be (false)
    ParquetIntStatistics(10, 20, 1L).isEqualToMax(20) should be (true)
  }

  test("ParquetIntStatistics - isEqualToMax, null") {
    ParquetIntStatistics(10, 20, 1L).isEqualToMax(null) should be (false)
    ParquetIntStatistics(10, 20, 0L).isEqualToMax(null) should be (false)
  }

  test("ParquetIntStatistics - isEqualToMax, other types") {
    ParquetIntStatistics(10, 20, 1L).isEqualToMax("abc") should be (false)
    ParquetIntStatistics(10, 20, 1L).isEqualToMax(Array(1, 2, 3)) should be (false)
  }

  // == Long statistics ==

  test("ParquetLongStatistics - invalid statistics") {
    var err = intercept[IllegalArgumentException] {
      ParquetLongStatistics(1L, -1L, 10L)
    }
    assert(err.getMessage.contains("Min 1 is expected to be less than or equal to max -1"))

    err = intercept[IllegalArgumentException] {
      ParquetLongStatistics(10L, 20L, -1L)
    }
    assert(err.getMessage.contains("Number of nulls -1 is negative"))
  }

  test("ParquetLongStatistics - valid statistics") {
    val stats = ParquetLongStatistics(128L, 256L, 1298L)
    stats.getMin should be (128L)
    stats.getMax should be (256L)
    stats.getNumNulls should be (1298L)
    stats.hasNull should be (true)
    stats.toString should be ("ParquetLongStatistics(min=128, max=256, nulls=1298)")
  }

  test("ParquetLongStatistics - valid not-null statitics") {
    val stats = ParquetLongStatistics(128L, 256L, 0L)
    stats.getMin should be (128L)
    stats.getMax should be (256L)
    stats.getNumNulls should be (0L)
    stats.hasNull should be (false)
  }

  test("ParquetLongStatistics - contains") {
    val stats = ParquetLongStatistics(Int.MinValue, Int.MaxValue, 120L)
    // different contains checks
    stats.contains(Long.MinValue) should be (false)
    stats.contains(Int.MinValue.toLong) should be (true)
    stats.contains(-1L) should be (true)
    stats.contains(7L) should be (true)
    stats.contains(96L) should be (true)
    stats.contains(128L) should be (true)
    stats.contains(Int.MaxValue.toLong) should be (true)
    stats.contains(Long.MaxValue) should be (false)
  }

  test("ParquetLongStatistics - contains nulls") {
    ParquetLongStatistics(1L, 2L, 0L).contains(null) should be (false)
    ParquetLongStatistics(1L, 2L, 1L).contains(null) should be (true)
  }

  test("ParquetLongStatistics - contains other types") {
    ParquetLongStatistics(1L, 2L, 200L).contains("str") should be (false)
    ParquetLongStatistics(1L, 2L, 200L).contains(Array(1, 2, 3)) should be (false)
    ParquetLongStatistics(1L, 2L, 200L).contains(true) should be (false)
    ParquetLongStatistics(1L, 2L, 200L).contains(false) should be (false)
    ParquetLongStatistics(1L, 2L, 200L).contains(Int.MinValue) should be (false)
    ParquetLongStatistics(1L, 2L, 200L).contains(Int.MaxValue) should be (false)
  }

  // long statistics - isLessThanMin

  test("ParquetLongStatistics - isLessThanMin") {
    ParquetLongStatistics(10L, 20L, 1L).isLessThanMin(9L) should be (true)
    ParquetLongStatistics(10L, 20L, 1L).isLessThanMin(10L) should be (false)
    ParquetLongStatistics(10L, 20L, 1L).isLessThanMin(20L) should be (false)
  }

  test("ParquetLongStatistics - isLessThanMin, null") {
    ParquetLongStatistics(10L, 20L, 1L).isLessThanMin(null) should be (false)
    ParquetLongStatistics(10L, 20L, 0L).isLessThanMin(null) should be (false)
  }

  test("ParquetLongStatistics - isLessThanMin, other types") {
    ParquetLongStatistics(10L, 20L, 1L).isLessThanMin("abc") should be (false)
    ParquetLongStatistics(10L, 20L, 1L).isLessThanMin(Array(1, 2, 3)) should be (false)
  }

  // long statistics - isEqualToMin

  test("ParquetLongStatistics - isEqualToMin") {
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMin(10L) should be (true)
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMin(20L) should be (false)
  }

  test("ParquetLongStatistics - isEqualToMin, null") {
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMin(null) should be (false)
    ParquetLongStatistics(10L, 20L, 0L).isEqualToMin(null) should be (false)
  }

  test("ParquetLongStatistics - isEqualToMin, other types") {
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMin("abc") should be (false)
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMin(Array(1, 2, 3)) should be (false)
  }

  // long statistics - isGreaterThanMax

  test("ParquetLongStatistics - isGreaterThanMax") {
    ParquetLongStatistics(10L, 20L, 1L).isGreaterThanMax(10L) should be (false)
    ParquetLongStatistics(10L, 20L, 1L).isGreaterThanMax(20L) should be (false)
    ParquetLongStatistics(10L, 20L, 1L).isGreaterThanMax(21L) should be (true)
  }

  test("ParquetLongStatistics - isGreaterThanMax, null") {
    ParquetLongStatistics(10L, 20L, 1L).isGreaterThanMax(null) should be (false)
    ParquetLongStatistics(10L, 20L, 0L).isGreaterThanMax(null) should be (false)
  }

  test("ParquetLongStatistics - isGreaterThanMax, other types") {
    ParquetLongStatistics(10L, 20L, 1L).isGreaterThanMax("abc") should be (false)
    ParquetLongStatistics(10L, 20L, 1L).isGreaterThanMax(Array(1, 2, 3)) should be (false)
  }

  // long statistics - isEqualToMax

  test("ParquetLongStatistics - isEqualToMax") {
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMax(10L) should be (false)
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMax(20L) should be (true)
  }

  test("ParquetLongStatistics - isEqualToMax, null") {
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMax(null) should be (false)
    ParquetLongStatistics(10L, 20L, 0L).isEqualToMax(null) should be (false)
  }

  test("ParquetLongStatistics - isEqualToMax, other types") {
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMax("abc") should be (false)
    ParquetLongStatistics(10L, 20L, 1L).isEqualToMax(Array(1, 2, 3)) should be (false)
  }

  // == String statistics ==

  test("ParquetStringStatistics - invalid statistics") {
    var err = intercept[IllegalArgumentException] {
      ParquetStringStatistics(null, "bbb", 10L)
    }
    assert(err.getMessage.contains("Min value cannot be null, set null count instead"))

    err = intercept[IllegalArgumentException] {
      ParquetStringStatistics("aaa", null, 10L)
    }
    assert(err.getMessage.contains("Max value cannot be null, set null count instead"))

    err = intercept[IllegalArgumentException] {
      ParquetStringStatistics("z", "a", 0L)
    }
    assert(err.getMessage.contains("Min z is greater than max a"))

    err = intercept[IllegalArgumentException] {
      ParquetStringStatistics("a", "z", -1L)
    }
    assert(err.getMessage.contains("Number of nulls -1 is negative"))
  }

  test("ParquetStringStatistics - valid statistics") {
    val stats = ParquetStringStatistics("a", "d", 123L)
    stats.getMin should be ("a")
    stats.getMax should be ("d")
    stats.getNumNulls should be (123L)
    stats.hasNull should be (true)
    stats.toString should be ("ParquetStringStatistics(min=a, max=d, nulls=123)")
  }

  test("ParquetStringStatistics - valid not-null statitics") {
    val stats = ParquetStringStatistics("a", "d", 0L)
    stats.getMin should be ("a")
    stats.getMax should be ("d")
    stats.getNumNulls should be (0L)
    stats.hasNull should be (false)
  }

  test("ParquetStringStatistics - contains") {
    val stats = ParquetStringStatistics("a", "d", 120L)
    // different contains checks
    stats.contains("1") should be (false)
    stats.contains("a") should be (true)
    stats.contains("aaa") should be (true)
    stats.contains("b") should be (true)
    stats.contains("bbb") should be (true)
    stats.contains("c") should be (true)
    stats.contains("ccc") should be (true)
    stats.contains("d") should be (true)
    stats.contains("ddd") should be (false)
    stats.contains("z") should be (false)
  }

  test("ParquetStringStatistics - contains nulls") {
    ParquetStringStatistics("a", "b", 0L).contains(null) should be (false)
    ParquetStringStatistics("a", "b", 1L).contains(null) should be (true)
  }

  test("ParquetStringStatistics - contains other types") {
    ParquetStringStatistics("a", "b", 200L).contains(Long.MinValue) should be (false)
    ParquetStringStatistics("a", "b", 200L).contains(Int.MinValue) should be (false)
    ParquetStringStatistics("a", "b", 200L).contains(Int.MaxValue) should be (false)
    ParquetStringStatistics("a", "b", 200L).contains(Long.MaxValue) should be (false)
    ParquetStringStatistics("a", "b", 200L).contains(Array(1, 2, 3)) should be (false)
    ParquetStringStatistics("a", "b", 200L).contains(true) should be (false)
    ParquetStringStatistics("a", "b", 200L).contains(false) should be (false)
  }

  // string statistics - isLessThanMin

  test("ParquetStringStatistics - isLessThanMin") {
    ParquetStringStatistics("b", "d", 1L).isLessThanMin("a") should be (true)
    ParquetStringStatistics("b", "d", 1L).isLessThanMin("aaaaaa") should be (true)
    ParquetStringStatistics("b", "d", 1L).isLessThanMin("b") should be (false)
    ParquetStringStatistics("b", "d", 1L).isLessThanMin("bbbbbb") should be (false)
    ParquetStringStatistics("b", "d", 1L).isLessThanMin("d") should be (false)
  }

  test("ParquetStringStatistics - isLessThanMin, null") {
    ParquetStringStatistics("b", "d", 1L).isLessThanMin(null) should be (false)
    ParquetStringStatistics("b", "d", 0L).isLessThanMin(null) should be (false)
  }

  test("ParquetStringStatistics - isLessThanMin, other types") {
    ParquetStringStatistics("b", "d", 1L).isLessThanMin(1L) should be (false)
    ParquetStringStatistics("b", "d", 1L).isLessThanMin(Array(1, 2, 3)) should be (false)
  }

  // string statistics - isEqualToMin

  test("ParquetStringStatistics - isEqualToMin") {
    ParquetStringStatistics("b", "d", 1L).isEqualToMin("b") should be (true)
    ParquetStringStatistics("b", "d", 1L).isEqualToMin("bb") should be (false)
    ParquetStringStatistics("b", "d", 1L).isEqualToMin("d") should be (false)
  }

  test("ParquetStringStatistics - isEqualToMin, null") {
    ParquetStringStatistics("b", "d", 1L).isEqualToMin(null) should be (false)
    ParquetStringStatistics("b", "d", 0L).isEqualToMin(null) should be (false)
  }

  test("ParquetStringStatistics - isEqualToMin, other types") {
    ParquetStringStatistics("b", "d", 1L).isEqualToMin(1L) should be (false)
    ParquetStringStatistics("b", "d", 1L).isEqualToMin(Array(1, 2, 3)) should be (false)
  }

  // string statistics - isGreaterThanMax

  test("ParquetStringStatistics - isGreaterThanMax") {
    ParquetStringStatistics("b", "d", 1L).isGreaterThanMax("a") should be (false)
    ParquetStringStatistics("b", "d", 1L).isGreaterThanMax("d") should be (false)
    ParquetStringStatistics("b", "d", 1L).isGreaterThanMax("dd") should be (true)
  }

  test("ParquetStringStatistics - isGreaterThanMax, null") {
    ParquetStringStatistics("b", "d", 1L).isGreaterThanMax(null) should be (false)
    ParquetStringStatistics("b", "d", 0L).isGreaterThanMax(null) should be (false)
  }

  test("ParquetStringStatistics - isGreaterThanMax, other types") {
    ParquetStringStatistics("b", "d", 1L).isGreaterThanMax(1L) should be (false)
    ParquetStringStatistics("b", "d", 1L).isGreaterThanMax(Array(1, 2, 3)) should be (false)
  }

  // string statistics - isEqualToMax

  test("ParquetStringStatistics - isEqualToMax") {
    ParquetStringStatistics("b", "d", 1L).isEqualToMax("b") should be (false)
    ParquetStringStatistics("b", "d", 1L).isEqualToMax("d") should be (true)
    ParquetStringStatistics("b", "d", 1L).isEqualToMax("dd") should be (false)
  }

  test("ParquetStringStatistics - isEqualToMax, null") {
    ParquetStringStatistics("b", "d", 1L).isEqualToMax(null) should be (false)
    ParquetStringStatistics("b", "d", 0L).isEqualToMax(null) should be (false)
  }

  test("ParquetStringStatistics - isEqualToMax, other types") {
    ParquetStringStatistics("b", "d", 1L).isEqualToMax(1L) should be (false)
    ParquetStringStatistics("b", "d", 1L).isEqualToMax(Array(1, 2, 3)) should be (false)
  }

  // == Filters ==

  test("ParquetBloomFilter - fail to check if not initialized") {
    val err = intercept[IllegalStateException] {
      ParquetBloomFilter("path").mightContain("str")
    }
    assert(err.getMessage.contains("Bloom filter is not initialized, call 'init()' first"))
  }

  test("ParquetBloomFilter - fail to initialize for invalid path") {
    withTempDir { dir =>
      val filter = ParquetBloomFilter("non-existent-path")
      val err = intercept[IOException] {
        filter.init(new Configuration(false))
      }
      assert(err.getMessage.contains("File non-existent-path does not exist"))
    }
  }

  test("ParquetBloomFilter - init/destroy") {
    withTempDir { dir =>
      val filter = BloomFilter.create(100, 0.05)
      filter.put(1)
      val out = create(dir.toString / "bloom.filter")
      filter.writeTo(out)
      out.close()

      val p = ParquetBloomFilter(dir.toString / "bloom.filter")
      p.isSet should be (false)

      p.init(new Configuration(false))
      p.isSet should be (true)

      p.destroy()
      p.isSet should be (false)
    }
  }

  test("ParquetBloomFilter - check validity of bloom filter") {
    withTempDir { dir =>
      val filter = BloomFilter.create(100, 0.05)
      filter.put(1)
      val out = create(dir.toString / "bloom.filter")
      filter.writeTo(out)
      out.close()

      val p = ParquetBloomFilter(dir.toString / "bloom.filter")
      p.init(new Configuration(false))

      // should return true for "1"
      p.mightContain(0) should be (false)
      p.mightContain(1) should be (true)
      p.mightContain(2) should be (false)
    }
  }

  test("ParquetBloomFilter - set filter") {
    val filter = BloomFilter.create(100, 0.05)
    filter.put(1)

    val p = ParquetBloomFilter("path")
    p.setBloomFilter(filter)
    p.isSet should be (true)
    p.mightContain(1) should be (true)
  }

  test("ParquetBloomFilter - init is no-op if filter is already set") {
    val filter = BloomFilter.create(100, 0.05)
    filter.put(1)

    val p = ParquetBloomFilter(null)
    p.setBloomFilter(filter)
    p.init(new Configuration(false))

    p.mightContain(1) should be (true)
  }

  test("ParquetBloomFilter - toString") {
    val filter = ParquetBloomFilter("/tmp/bloom.filter")
    filter.toString should be ("ParquetBloomFilter(path=/tmp/bloom.filter)")
  }

  test("ParquetColumnMetadata - withFilter") {
    val meta = ParquetColumnMetadata("field", 100, ParquetIntStatistics(0, 1, 0L), None)
    meta.withFilter(None).filter should be (None)
    meta.withFilter(Some(null)).filter should be (None)
    meta.withFilter(Some(ParquetBloomFilter("path"))).filter should be (
      Some(ParquetBloomFilter("path")))
  }

  test("ParquetFileStatus - numRows for empty blocks") {
    val status = ParquetFileStatus(status = null, "schema", Array.empty)
    status.numRows should be (0)
  }

  test("ParquetFileStatus - numRows for single block") {
    val status = ParquetFileStatus(status = null, "schema",
      Array(ParquetBlockMetadata(123, Map.empty)))
    status.numRows should be (123)
  }

  test("ParquetFileStatus - numRows for non-empty blocks") {
    val status = ParquetFileStatus(status = null, "schema", Array(
      ParquetBlockMetadata(11, Map.empty),
      ParquetBlockMetadata(12, Map.empty),
      ParquetBlockMetadata(13, Map.empty)))
    status.numRows should be (36)
  }
}
