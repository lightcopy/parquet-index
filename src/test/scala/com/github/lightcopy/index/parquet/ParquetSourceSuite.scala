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

package com.github.lightcopy.index.parquet

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test suite for Parquet source and index */
class ParquetSourceSuite extends UnitTestSuite {
  // currently all tests throw unsupported exception at the time
  test("fail when loading index") {
    val source = new ParquetSource()
    intercept[UnsupportedOperationException] {
      source.loadIndex(null, null)
    }
  }

  test("fail when creating index") {
    val source = new ParquetSource()
    intercept[UnsupportedOperationException] {
      source.createIndex(null, null, null, null)
    }
  }

  test("fail when fallback") {
    val source = new ParquetSource()
    intercept[UnsupportedOperationException] {
      source.fallback(null, null, null)
    }
  }
}
