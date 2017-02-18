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

package org.apache.spark.sql.sources

import org.apache.spark.sql.types._

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class ColumnFilterStatisticsSuite extends UnitTestSuite {
  test("ColumnFilterStatistics - getColumnFilter for filter type and data type") {
    ColumnFilterStatistics.getColumnFilter(StringType, ColumnFilterStatistics.BLOOM_FILTER_TYPE,
      1L).getClass should be (classOf[BloomFilterStatistics])
    ColumnFilterStatistics.getColumnFilter(LongType, ColumnFilterStatistics.BLOOM_FILTER_TYPE,
      1L).getClass should be (classOf[BloomFilterStatistics])
    ColumnFilterStatistics.getColumnFilter(IntegerType, ColumnFilterStatistics.BLOOM_FILTER_TYPE,
      1L).getClass should be (classOf[BloomFilterStatistics])

    ColumnFilterStatistics.getColumnFilter(StringType, ColumnFilterStatistics.DICT_FILTER_TYPE,
      1L).getClass should be (classOf[DictionaryFilterStatistics])
    ColumnFilterStatistics.getColumnFilter(LongType, ColumnFilterStatistics.DICT_FILTER_TYPE,
      1L).getClass should be (classOf[DictionaryFilterStatistics])
    ColumnFilterStatistics.getColumnFilter(IntegerType, ColumnFilterStatistics.DICT_FILTER_TYPE,
      1L).getClass should be (classOf[BitmapFilterStatistics])
  }

  test("ColumnFilterStatistics - getColumnFilter, throw error if name is not registered") {
    var err = intercept[RuntimeException] {
      ColumnFilterStatistics.getColumnFilter(StringType, "BLOOM", 1L)
    }
    assert(err.getMessage.contains("Unsupported filter statistics type BLOOM"))

    err = intercept[RuntimeException] {
      ColumnFilterStatistics.getColumnFilter(StringType, "DICT", 1L)
    }
    assert(err.getMessage.contains("Unsupported filter statistics type DICT"))

    err = intercept[RuntimeException] {
      ColumnFilterStatistics.getColumnFilter(StringType, "test", 1L)
    }
    assert(err.getMessage.contains("Unsupported filter statistics type test"))

    err = intercept[RuntimeException] {
      ColumnFilterStatistics.getColumnFilter(StringType, "", 1L)
    }
    assert(err.getMessage.contains("Unsupported filter statistics type "))
  }

  test("FilterStatisticsMetadata - init") {
    val meta = new FilterStatisticsMetadata()
    meta.enabled should be (false)
  }

  test("FilterStatisticsMetadata - setDirectory/getDirectory/getPath, fail for disabled") {
    // set None as directory
    val meta = new FilterStatisticsMetadata()
    meta.setDirectory(None)
    val err = intercept[IllegalArgumentException] {
      meta.getDirectory()
    }
    assert(err.getMessage.contains("Failed to extract directory for disabled metadata"))
  }

  test("FilterStatisticsMetadata - setFilterType/getFilterType, fail for unsupported type") {
    val meta = new FilterStatisticsMetadata()
    meta.setFilterType(None)
    var err = intercept[IllegalArgumentException] {
      meta.getFilterType()
    }
    assert(err.getMessage.contains("Failed to extract filter type for disabled metadata"))

    meta.setFilterType(Some("test"))
    err = intercept[IllegalArgumentException] {
      meta.getFilterType()
    }
    assert(err.getMessage.contains("Failed to extract filter type for disabled metadata"))
  }

  test("FilterStatisticsMetadata - setters and getters") {
    withTempDir { dir =>
      var status = fs.getFileStatus(dir)
      val meta = new FilterStatisticsMetadata()
      meta.setDirectory(Some(status))
      meta.setFilterType(Some("bloom"))
      meta.getDirectory() should be (status)
      meta.getPath() should be (status.getPath)
      meta.getFilterType() should be ("bloom")
    }
  }

  test("FilterStatisticsMetadata - enabled, if directory and type are set") {
    withTempDir { dir =>
      var status = fs.getFileStatus(dir)
      val meta = new FilterStatisticsMetadata()
      meta.setDirectory(Some(status))
      meta.setFilterType(Some("bloom"))
      meta.enabled should be (true)
    }
  }

  test("FilterStatisticsMetadata - disabled, if directory or type are not set") {
    withTempDir { dir =>
      var status = fs.getFileStatus(dir)
      val meta = new FilterStatisticsMetadata()
      meta.enabled should be (false)

      meta.setDirectory(Some(status))
      meta.setFilterType(None)
      meta.enabled should be (false)

      meta.setDirectory(None)
      meta.setFilterType(Some("bloom"))
      meta.enabled should be (false)

      meta.setDirectory(Some(status))
      meta.setFilterType(Some("bloom"))
      meta.enabled should be (true)
    }
  }

  test("FilterStatisticsMetadata - toString") {
    withTempDir { dir =>
      var status = fs.getFileStatus(dir)
      val meta = new FilterStatisticsMetadata()
      meta.toString should be ("(enabled=false, none)")

      meta.setDirectory(Some(status))
      meta.setFilterType(None)
      meta.toString should be ("(enabled=false, none)")

      meta.setDirectory(None)
      meta.setFilterType(Some("bloom"))
      meta.toString should be ("(enabled=false, none)")

      meta.setDirectory(Some(status))
      meta.setFilterType(Some("bloom"))
      meta.toString should be (s"(enabled=true, directory=$status, type=bloom)")
    }
  }

  test("BloomFilterStatistics - initialize") {
    val filter = BloomFilterStatistics()
    filter.getNumRows should be (1024)
    filter.isLoaded should be (false)
  }

  test("BloomFilterStatistics - initialize with numRows") {
    val filter = BloomFilterStatistics(1024L * 1024L)
    filter.getNumRows should be (1024L * 1024L)
    filter.isLoaded should be (false)
  }

  test("BloomFilterStatistics - toString") {
    val filter = BloomFilterStatistics()
    filter.toString should be ("BloomFilterStatistics")
  }

  test("BloomFilterStatistics - fail if numRows is non-positive") {
    var err = intercept[IllegalArgumentException] {
      BloomFilterStatistics(-1)
    }
    assert(err.getMessage.contains("Invalid expected number of records -1, should be > 0"))

    err = intercept[IllegalArgumentException] {
      BloomFilterStatistics(0)
    }
    assert(err.getMessage.contains("Invalid expected number of records 0, should be > 0"))
  }

  test("BloomFilterStatistics - setPath/getPath") {
    withTempDir { dir =>
      val filter = BloomFilterStatistics()
      filter.setPath(dir)
      filter.getPath should be (dir)
    }
  }

  test("BloomFilterStatistics - fail to set null path") {
    val filter = BloomFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.setPath(null)
    }
  }

  test("BloomFilterStatistics - fail to extract path before it is set") {
    val filter = BloomFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.getPath
    }
  }

  test("BloomFilterStatistics - update/mightContain") {
    val filter = BloomFilterStatistics()
    for (i <- 1 to 1024) {
      filter.update(i)
    }
    filter.mightContain(1) should be (true)
    filter.mightContain(1024) should be (true)
    filter.mightContain(1025) should be (false)
    filter.mightContain("abc") should be (false)
    // bloom filter returns "true" since long would map to the same bytes as int by
    // discarding leading zeros
    filter.mightContain(1024L) should be (true)
  }

  test("BloomFilterStatistics - supported types") {
    for (i <- Seq(1, 1L, "abc", new java.sql.Date(1L), new java.sql.Timestamp(1L))) {
      val filter = BloomFilterStatistics()
      filter.update(i)
      filter.mightContain(i) should be (true)
    }
  }

  test("BloomFilterStatistics - unsupported types") {
    val filter = BloomFilterStatistics()
    // boolean is not supported by bloom filter
    var err = intercept[UnsupportedOperationException] {
      filter.mightContain(true)
    }
    assert(err.getMessage.contains("BloomFilterStatistics does not support value true"))
  }

  test("BloomFilterStatistics - mightContain on empty filter") {
    val filter = BloomFilterStatistics()
    filter.mightContain(1) should be (false)
    filter.mightContain(1024) should be (false)
    filter.mightContain(1025) should be (false)
  }

  test("BloomFilterStatistics - writeData/readData") {
    withTempDir { dir =>
      val filter = BloomFilterStatistics()
      for (i <- 1 to 1024) {
        filter.update(i)
      }
      filter.setPath(dir / "filter")
      filter.writeData(fs)

      fs.exists(filter.getPath) should be (true)

      val filter2 = BloomFilterStatistics()
      filter2.setPath(dir / "filter")
      filter2.readData(fs)
      filter2.isLoaded should be (true)

      filter2.mightContain(1) should be (true)
      filter2.mightContain(1024) should be (true)
      filter2.mightContain(1025) should be (false)
    }
  }

  test("BloomFilterStatistics - fail to write data for null path, do not close null stream") {
    val filter = BloomFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.writeData(fs)
    }
  }

  test("BloomFilterStatistics - fail to read data for null path, do not close null stream") {
    val filter = BloomFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.readData(fs)
    }
  }

  test("BloomFilterStatistics - call 'readData' multiple times") {
    withTempDir { dir =>
      val filter = BloomFilterStatistics()
      filter.update(1)
      filter.setPath(dir / "filter")
      filter.writeData(fs)

      val filter2 = BloomFilterStatistics()
      filter2.setPath(dir / "filter")
      filter2.readData(fs)
      filter2.isLoaded should be (true)
      filter2.mightContain(1) should be (true)

      // delete path
      fs.delete(filter2.getPath, true) should be (true)
      // this call should result in no-op and use already instantiated filter
      filter2.readData(fs)
      filter2.isLoaded should be (true)
      filter2.mightContain(1) should be (true)
    }
  }

  test("DictionaryFilterStatistics - initialize") {
    val filter = DictionaryFilterStatistics()
    filter.getSet.isEmpty should be (true)
    filter.isLoaded should be (false)
  }

  test("DictionaryFilterStatistics - toString") {
    val filter = DictionaryFilterStatistics()
    filter.toString should be ("DictionaryFilterStatistics")
  }

  test("DictionaryFilterStatistics - setPath/getPath") {
    withTempDir { dir =>
      val filter = DictionaryFilterStatistics()
      filter.setPath(dir)
      filter.getPath should be (dir)
    }
  }

  test("DictionaryFilterStatistics - fail to set null path") {
    val filter = DictionaryFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.setPath(null)
    }
  }

  test("DictionaryFilterStatistics - fail to extract path before it is set") {
    val filter = DictionaryFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.getPath
    }
  }

  test("DictionaryFilterStatistics - update/mightContain") {
    val filter = DictionaryFilterStatistics()
    for (i <- 1 to 1024) {
      filter.update(i)
    }
    filter.mightContain(1) should be (true)
    filter.mightContain(1024) should be (true)
    filter.mightContain(1025) should be (false)
    filter.mightContain("abc") should be (false)
    filter.mightContain(1024L) should be (false)
  }

  test("DictionaryFilterStatistics - supported types") {
    for (i <- Seq(1, 1L, "abc", new java.sql.Date(1L), new java.sql.Timestamp(1L))) {
      val filter = DictionaryFilterStatistics()
      filter.update(i)
      filter.mightContain(i) should be (true)
    }
  }

  test("DictionaryFilterStatistics - unsupported types") {
    val filter = DictionaryFilterStatistics()
    // boolean is not supported by dictionary filter
    var err = intercept[UnsupportedOperationException] {
      filter.mightContain(true)
    }
    assert(err.getMessage.contains("DictionaryFilterStatistics does not support value true"))
  }

  test("DictionaryFilterStatistics - mightContain on empty filter") {
    val filter = DictionaryFilterStatistics()
    filter.mightContain(1) should be (false)
    filter.mightContain(1024) should be (false)
    filter.mightContain(1025) should be (false)
  }

  test("DictionaryFilterStatistics - writeData/readData") {
    withTempDir { dir =>
      val filter = DictionaryFilterStatistics()
      for (i <- 1 to 1024) {
        filter.update(i)
      }
      filter.setPath(dir / "filter")
      filter.writeData(fs)

      fs.exists(filter.getPath) should be (true)

      val filter2 = DictionaryFilterStatistics()
      filter2.setPath(dir / "filter")
      filter2.readData(fs)
      filter2.isLoaded should be (true)

      filter2.mightContain(1) should be (true)
      filter2.mightContain(1024) should be (true)
      filter2.mightContain(1025) should be (false)
    }
  }

  test("DictionaryFilterStatistics - fail to write data for null path, do not close null stream") {
    val filter = DictionaryFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.writeData(fs)
    }
  }

  test("DictionaryFilterStatistics - fail to read data for null path, do not close null stream") {
    val filter = DictionaryFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.readData(fs)
    }
  }

  test("DictionaryFilterStatistics - call 'readData' multiple times") {
    withTempDir { dir =>
      val filter = DictionaryFilterStatistics()
      filter.update(1)
      filter.setPath(dir / "filter")
      filter.writeData(fs)

      val filter2 = DictionaryFilterStatistics()
      filter2.setPath(dir / "filter")
      filter2.readData(fs)
      filter2.isLoaded should be (true)
      filter2.mightContain(1) should be (true)

      // delete path
      fs.delete(filter2.getPath, true) should be (true)
      // this call should result in no-op and use already instantiated filter
      filter2.readData(fs)
      filter2.isLoaded should be (true)
      filter2.mightContain(1) should be (true)
    }
  }

  test("BitmapFilterStatistics - initialize") {
    val filter = BitmapFilterStatistics()
    filter.isLoaded should be (false)
  }

  test("BitmapFilterStatistics - toString") {
    val filter = BitmapFilterStatistics()
    filter.toString should be ("BitmapFilterStatistics")
  }

  test("BitmapFilterStatistics - setPath/getPath") {
    withTempDir { dir =>
      val filter = BitmapFilterStatistics()
      filter.setPath(dir)
      filter.getPath should be (dir)
    }
  }

  test("BitmapFilterStatistics - fail to set null path") {
    val filter = BitmapFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.setPath(null)
    }
  }

  test("BitmapFilterStatistics - fail to extract path before it is set") {
    val filter = BitmapFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.getPath
    }
  }

  test("BitmapFilterStatistics - update/mightContain") {
    val filter = BitmapFilterStatistics()
    for (i <- 1 to 1024) {
      filter.update(i)
    }
    filter.mightContain(1) should be (true)
    filter.mightContain(1024) should be (true)
    filter.mightContain(1025) should be (false)
  }

  test("BitmapFilterStatistics - unsupported types") {
    val filter = BitmapFilterStatistics()
    var err = intercept[UnsupportedOperationException] {
      filter.mightContain(true)
    }
    assert(err.getMessage.contains("BitmapFilterStatistics does not support value true"))

    err = intercept[UnsupportedOperationException] {
      filter.mightContain("abc")
    }
    assert(err.getMessage.contains("BitmapFilterStatistics does not support value abc"))

    err = intercept[UnsupportedOperationException] {
      filter.mightContain(1L)
    }
    assert(err.getMessage.contains("BitmapFilterStatistics does not support value 1"))
  }

  test("BitmapFilterStatistics - mightContain on empty filter") {
    val filter = BitmapFilterStatistics()
    filter.mightContain(1) should be (false)
    filter.mightContain(1024) should be (false)
    filter.mightContain(1025) should be (false)
  }

  test("BitmapFilterStatistics - writeData/readData") {
    withTempDir { dir =>
      val filter = BitmapFilterStatistics()
      for (i <- 1 to 1024) {
        filter.update(i)
      }
      filter.setPath(dir / "filter")
      filter.writeData(fs)

      fs.exists(filter.getPath) should be (true)

      val filter2 = BitmapFilterStatistics()
      filter2.setPath(dir / "filter")
      filter2.readData(fs)
      filter2.isLoaded should be (true)

      filter2.mightContain(1) should be (true)
      filter2.mightContain(1024) should be (true)
      filter2.mightContain(1025) should be (false)
    }
  }

  test("BitmapFilterStatistics - fail to write data for null path, do not close null stream") {
    val filter = BitmapFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.writeData(fs)
    }
  }

  test("BitmapFilterStatistics - fail to read data for null path, do not close null stream") {
    val filter = BitmapFilterStatistics()
    intercept[IllegalArgumentException] {
      filter.readData(fs)
    }
  }

  test("BitmapFilterStatistics - call 'readData' multiple times") {
    withTempDir { dir =>
      val filter = BitmapFilterStatistics()
      filter.update(1)
      filter.setPath(dir / "filter")
      filter.writeData(fs)

      val filter2 = BitmapFilterStatistics()
      filter2.setPath(dir / "filter")
      filter2.readData(fs)
      filter2.isLoaded should be (true)
      filter2.mightContain(1) should be (true)

      // delete path
      fs.delete(filter2.getPath, true) should be (true)
      // this call should result in no-op and use already instantiated filter
      filter2.readData(fs)
      filter2.isLoaded should be (true)
      filter2.mightContain(1) should be (true)
    }
  }
}
