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

package com.github.lightcopy.index.simple

import com.github.lightcopy.index.Metadata
import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test suite for Simple source and index */
class SimpleSourceSuite extends UnitTestSuite {

  test("verify simple index") {
    val index = new SimpleIndex(null)
    index.getName should be ("simple")
    index.getRoot should be ("file:/root/simple")
    index.getMetadata should be (Metadata("simple", None, Seq.empty, Map.empty))
    // we pass null catalog
    index.catalog should be (null)
    // always returns true
    index.containsSpec(null) should be (true)
    // regardless of condition always returns null DataFrame
    index.search(null) should be (null)
  }

  test("load simple index") {
    val source = new SimpleSource()
    val index = source.loadIndex(null, null)
    index.isInstanceOf[SimpleIndex] should be (true)
  }

  test("create simple index") {
    val source = new SimpleSource()
    val index = source.createIndex(null, null, null, Seq.empty)
    index.isInstanceOf[SimpleIndex] should be (true)
  }

  test("simple source fallback") {
    val source = new SimpleSource()
    val df = source.fallback(null, null, null)
    df should be (null)
  }
}
