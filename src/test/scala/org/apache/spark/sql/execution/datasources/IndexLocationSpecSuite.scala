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

package org.apache.spark.sql.execution.datasources

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test location spec for validation */
private[datasources] case class TestLocationSpec(
    unresolvedIndentifier: String,
    unresolvedDataspace: String) extends IndexLocationSpec

class IndexLocationSpecSuite extends UnitTestSuite {
  test("IndexLocationSpec - empty/null identifier") {
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec(null, "value").identifier
    }
    assert(err.getMessage.contains("Empty identifier"))

    err = intercept[IllegalArgumentException] {
      TestLocationSpec("", "value").identifier
    }
    assert(err.getMessage.contains("Empty identifier"))
  }

  test("IndexLocationSpec - empty/null dataspace") {
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec("value", null).dataspace
    }
    assert(err.getMessage.contains("Empty dataspace"))

    err = intercept[IllegalArgumentException] {
      TestLocationSpec("value", "").dataspace
    }
    assert(err.getMessage.contains("Empty dataspace"))
  }

  test("IndexLocationSpec - invalid set of characters") {
    // identifier contains spaces
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec("test ", "ok").identifier
    }
    assert(err.getMessage.contains("Invalid character   in identifier test "))

    // all characters are invalid
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("#$%", "ok").identifier
    }
    assert(err.getMessage.contains("Invalid character # in identifier #$%"))

    // identifier contains uppercase characters
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("Test", "ok").identifier
    }
    assert(err.getMessage.contains("Invalid character T in identifier Test"))

    // identifier contains underscore
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("test_123", "ok").identifier
    }
    assert(err.getMessage.contains("Invalid character _ in identifier test_123"))

    // identifier contains hyphen
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("test-123", "ok").identifier
    }
    assert(err.getMessage.contains("Invalid character - in identifier test-123"))
  }

  test("IndexLocationSpec - valid identifier") {
    TestLocationSpec("test", "test").identifier
    TestLocationSpec("test1239", "test1239").identifier
    TestLocationSpec("012345689", "012345689").identifier
    TestLocationSpec("0123test", "0123test").identifier
  }

  test("IndexLocationSpec - source spec") {
    val spec = SourceLocationSpec("parquet")
    spec.identifier should be ("parquet")
    spec.dataspace should be ("source")
  }

  test("IndexLocationSpec - catalog spec") {
    val spec = CatalogLocationSpec("parquet")
    spec.identifier should be ("parquet")
    spec.dataspace should be ("catalog")
  }

  test("IndexLocationSpec - toString") {
    TestLocationSpec("identifier", "dataspace").toString should be ("[dataspace/identifier]")
    SourceLocationSpec("parquet").toString should be ("[source/parquet]")
    CatalogLocationSpec("parquet").toString should be ("[catalog/parquet]")
  }
}
