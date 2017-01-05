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

import org.apache.hadoop.fs.Path

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class ColumnFilterStatisticsSuite extends UnitTestSuite {
  test("ColumnFilterStatistics - classForName, select BloomFilterStatistics") {
    ColumnFilterStatistics.classForName("bloom") should be (classOf[BloomFilterStatistics])
  }

  test("ColumnFilterStatistics - classForName, throw error if name is not registered") {
    var err = intercept[RuntimeException] {
      ColumnFilterStatistics.classForName("BLOOM")
    }
    assert(err.getMessage.contains("Unsupported filter statistics type BLOOM"))

    err = intercept[RuntimeException] {
      ColumnFilterStatistics.classForName("dict")
    }
    assert(err.getMessage.contains("Unsupported filter statistics type dict"))

    err = intercept[RuntimeException] {
      ColumnFilterStatistics.classForName("")
    }
    assert(err.getMessage.contains("Unsupported filter statistics type "))
  }

  test("BloomFilterStatistics - initialize") {
    val filter = BloomFilterStatistics()
    filter.getNumRows should be (1024)
    filter.getHasLoadedData should be (false)
  }

  test("BloomFilterStatistics - initialize with numRows") {
    val filter = BloomFilterStatistics(1024L * 1024L)
    filter.getNumRows should be (1024L * 1024L)
    filter.getHasLoadedData should be (false)
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
    val filter = BloomFilterStatistics()
    filter.setPath(new Path("/tmp/filter"))
    filter.getPath should be (new Path("/tmp/filter"))
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
      filter.setPath(new Path(dir.toString / "filter"))
      filter.writeData(fs)

      fs.exists(filter.getPath) should be (true)

      val filter2 = BloomFilterStatistics()
      filter2.setPath(new Path(dir.toString / "filter"))
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
      for (i <- 1 to 1024) {
        filter.update(i)
      }
      filter.setPath(new Path(dir.toString / "filter"))
      filter.writeData(fs)

      val filter2 = BloomFilterStatistics()
      filter2.setPath(new Path(dir.toString / "filter"))
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
