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

import org.apache.spark.sql.SaveMode

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test suite for [[Catalog]], [[InternalCatalog]] and [[IndexSpec]] */
class CatalogSuite extends UnitTestSuite {
  test("index spec - toString 1") {
    val spec = IndexSpec("test-source", Some("path"), SaveMode.Append, Map.empty)
    spec.toString should be (
      "IndexSpec(source=test-source, path=Some(path), mode=Append, options=Map())")
  }

  test("index spec - toString 2") {
    val spec = IndexSpec("test-source", None, SaveMode.Ignore, Map.empty)
    spec.toString should be (
      "IndexSpec(source=test-source, path=None, mode=Ignore, options=Map())")
  }

  test("index spec - toString 3") {
    val spec = IndexSpec("test-source", None, SaveMode.Ignore, Map("1" -> "2", "3" -> "4"))
    spec.toString should be (
      "IndexSpec(source=test-source, path=None, mode=Ignore, options=Map(1 -> 2, 3 -> 4))")
  }
}
