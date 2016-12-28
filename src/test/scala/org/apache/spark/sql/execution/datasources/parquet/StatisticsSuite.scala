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

  test("ParquetIntStatistics - isLessThan") {
    ParquetIntStatistics(7, 128, 200L).isLessThan(129) should be (true)
    ParquetIntStatistics(7, 128, 200L).isLessThan(128) should be (false)
    ParquetIntStatistics(7, 128, 200L).isLessThan(10) should be (false)
    ParquetIntStatistics(7, 128, 200L).isLessThan(1) should be (false)
  }

  test("ParquetIntStatistics - isLessThan, null") {
    ParquetIntStatistics(7, 128, 200L).isLessThan(null) should be (false)
    ParquetIntStatistics(7, 128, 200L).isLessThan(null) should be (false)
  }

  test("ParquetIntStatistics - isLessThan, other types") {
    ParquetIntStatistics(7, 128, 200L).isLessThan("abc") should be (false)
    ParquetIntStatistics(7, 128, 200L).isLessThan(Array(1, 2, 3)) should be (false)
  }

  test("ParquetIntStatistics - isLessThanOrEqual") {
    ParquetIntStatistics(7, 128, 200L).isLessThanOrEqual(129) should be (true)
    ParquetIntStatistics(7, 128, 200L).isLessThanOrEqual(128) should be (true)
    ParquetIntStatistics(7, 128, 200L).isLessThanOrEqual(10) should be (false)
    ParquetIntStatistics(7, 128, 200L).isLessThanOrEqual(1) should be (false)
  }

  test("ParquetIntStatistics - isLessThanOrEqual, null") {
    ParquetIntStatistics(7, 128, 200L).isLessThanOrEqual(null) should be (false)
    ParquetIntStatistics(7, 128, 200L).isLessThanOrEqual(null) should be (false)
  }

  test("ParquetIntStatistics - isLessThanOrEqual, other types") {
    ParquetIntStatistics(7, 128, 200L).isLessThanOrEqual("abc") should be (false)
    ParquetIntStatistics(7, 128, 200L).isLessThanOrEqual(Array(1, 2, 3)) should be (false)
  }

  test("ParquetIntStatistics - isGreaterThan") {
    ParquetIntStatistics(7, 128, 200L).isGreaterThan(129) should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThan(128) should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThan(7) should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThan(1) should be (true)
  }

  test("ParquetIntStatistics - isGreaterThan, null") {
    ParquetIntStatistics(7, 128, 200L).isGreaterThan(null) should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThan(null) should be (false)
  }

  test("ParquetIntStatistics - isGreaterThan, other types") {
    ParquetIntStatistics(7, 128, 200L).isGreaterThan("abc") should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThan(Array(1, 2, 3)) should be (false)
  }

  test("ParquetIntStatistics - isGreaterThanOrEqual") {
    ParquetIntStatistics(7, 128, 200L).isGreaterThanOrEqual(129) should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThanOrEqual(128) should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThanOrEqual(7) should be (true)
    ParquetIntStatistics(7, 128, 200L).isGreaterThanOrEqual(1) should be (true)
  }

  test("ParquetIntStatistics - isGreaterThanOrEqual, null") {
    ParquetIntStatistics(7, 128, 200L).isGreaterThanOrEqual(null) should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThanOrEqual(null) should be (false)
  }

  test("ParquetIntStatistics - isGreaterThanOrEqual, other types") {
    ParquetIntStatistics(7, 128, 200L).isGreaterThanOrEqual("abc") should be (false)
    ParquetIntStatistics(7, 128, 200L).isGreaterThanOrEqual(Array(1, 2, 3)) should be (false)
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

  test("ParquetLongStatistics - isLessThan") {
    ParquetLongStatistics(100L, 128L, 200L).isLessThan(129L) should be (true)
    ParquetLongStatistics(100L, 128L, 200L).isLessThan(128L) should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isLessThan(100L) should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isLessThan(1L) should be (false)
  }

  test("ParquetLongStatistics - isLessThan, null") {
    ParquetLongStatistics(100L, 128L, 200L).isLessThan(null) should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isLessThan(null) should be (false)
  }

  test("ParquetLongStatistics - isLessThan, other types") {
    ParquetLongStatistics(100L, 128L, 200L).isLessThan("abc") should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isLessThan(Array(1, 2, 3)) should be (false)
  }

  test("ParquetLongStatistics - isLessThanOrEqual") {
    ParquetLongStatistics(100L, 128L, 200L).isLessThanOrEqual(129L) should be (true)
    ParquetLongStatistics(100L, 128L, 200L).isLessThanOrEqual(128L) should be (true)
    ParquetLongStatistics(100L, 128L, 200L).isLessThanOrEqual(100L) should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isLessThanOrEqual(1L) should be (false)
  }

  test("ParquetLongStatistics - isLessThanOrEqual, null") {
    ParquetLongStatistics(100L, 128L, 200L).isLessThanOrEqual(null) should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isLessThanOrEqual(null) should be (false)
  }

  test("ParquetLongStatistics - isLessThanOrEqual, other types") {
    ParquetLongStatistics(100L, 128L, 200L).isLessThanOrEqual("abc") should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isLessThanOrEqual(Array(1, 2, 3)) should be (false)
  }

  test("ParquetLongStatistics - isGreaterThan") {
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThan(99L) should be (true)
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThan(100L) should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThan(128L) should be (false)
  }

  test("ParquetLongStatistics - isGreaterThan, null") {
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThan(null) should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThan(null) should be (false)
  }

  test("ParquetLongStatistics - isGreaterThan, other types") {
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThan("abc") should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThan(Array(1, 2, 3)) should be (false)
  }

  test("ParquetLongStatistics - isGreaterThanOrEqual") {
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThanOrEqual(99L) should be (true)
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThanOrEqual(100L) should be (true)
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThanOrEqual(128L) should be (false)
  }

  test("ParquetLongStatistics - isGreaterThanOrEqual, null") {
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThanOrEqual(null) should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThanOrEqual(null) should be (false)
  }

  test("ParquetLongStatistics - isGreaterThanOrEqual, other types") {
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThanOrEqual("abc") should be (false)
    ParquetLongStatistics(100L, 128L, 200L).isGreaterThanOrEqual(Array(1, 2, 3)) should be (false)
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

  test("ParquetStringStatistics - isLessThan") {
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan("dddd") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan("e") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan("ddd") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan("aaa") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan("abc") should be (false)
  }

  test("ParquetStringStatistics - isLessThan, null") {
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan(null) should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan(null) should be (false)
  }

  test("ParquetStringStatistics - isLessThan, other types") {
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan("abc") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThan(Array(1, 2, 3)) should be (false)
  }

  test("ParquetStringStatistics - isLessThanOrEqual") {
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual("dddd") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual("e") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual("ddd") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual("aaa") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual("abc") should be (false)
  }

  test("ParquetStringStatistics - isLessThanOrEqual, null") {
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual(null) should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual(null) should be (false)
  }

  test("ParquetStringStatistics - isLessThanOrEqual, other types") {
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual("abc") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isLessThanOrEqual(Array(1, 2, 3)) should be (false)
  }

  test("ParquetStringStatistics - isGreaterThan") {
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan("123") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan("aa") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan("aaa") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan("ddd") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan("abc") should be (false)
  }

  test("ParquetStringStatistics - isGreaterThan, null") {
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan(null) should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan(null) should be (false)
  }

  test("ParquetStringStatistics - isGreaterThan, other types") {
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan("abc") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThan(Array(1, 2, 3)) should be (false)
  }

  test("ParquetStringStatistics - isGreaterThanOrEqual") {
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThanOrEqual("123") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThanOrEqual("aa") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThanOrEqual("aaa") should be (true)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThanOrEqual("ddd") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThanOrEqual("abc") should be (false)
  }

  test("ParquetStringStatistics - isGreaterThanOrEqual, null") {
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThanOrEqual(null) should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).isGreaterThanOrEqual(null) should be (false)
  }

  test("ParquetStringStatistics - isGreaterThanOrEqual, other types") {
    ParquetStringStatistics("aaa", "ddd", 200L).
      isGreaterThanOrEqual("abc") should be (false)
    ParquetStringStatistics("aaa", "ddd", 200L).
      isGreaterThanOrEqual(Array(1, 2, 3)) should be (false)
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
