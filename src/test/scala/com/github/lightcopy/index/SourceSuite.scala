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

package com.github.lightcopy.index

import org.apache.spark.sql.Column

import com.github.lightcopy.{Catalog, IndexSpec}
import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test suite for [[Source]] and interfaces */
class SourceSuite extends UnitTestSuite {
  test("index source default fallback behaviour") {
    // should throw an unsupported exception
    val source = new IndexSource() {
      override def loadIndex(catalog: Catalog, metadata: Metadata): Index = ???
      override def createIndex(
        catalog: Catalog, spec: IndexSpec, dir: String, columns: Seq[Column]): Index = ???
    }

    intercept[UnsupportedOperationException] {
      // parameters do not really matter, no validation is done
      source.fallback(null, null, null)
    }
  }
}
