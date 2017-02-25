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

import org.apache.hadoop.fs.Path

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test location spec for validation */
private[datasources] case class TestLocationSpec(
    unresolvedIndentifier: String,
    unresolvedDataspace: String,
    sourcePath: Path) extends IndexLocationSpec

class IndexLocationSpecSuite extends UnitTestSuite {
  test("IndexLocationSpec - empty/null identifier") {
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec(null, "value", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Empty identifier"))

    err = intercept[IllegalArgumentException] {
      TestLocationSpec("", "value", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Empty identifier"))
  }

  test("IndexLocationSpec - empty/null dataspace") {
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec("value", null, new Path(".")).dataspace
    }
    assert(err.getMessage.contains("Empty dataspace"))

    err = intercept[IllegalArgumentException] {
      TestLocationSpec("value", "", new Path(".")).dataspace
    }
    assert(err.getMessage.contains("Empty dataspace"))
  }

  test("IndexLocationSpec - invalid set of characters") {
    // identifier contains spaces
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec("test ", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character   in identifier test "))

    // all characters are invalid
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("#$%", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character # in identifier #$%"))

    // identifier contains uppercase characters
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("Test", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character T in identifier Test"))

    // identifier contains underscore
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("test_123", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character _ in identifier test_123"))

    // identifier contains hyphen
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("test-123", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character - in identifier test-123"))
  }

  test("IndexLocationSpec - valid identifier") {
    // test identifier
    TestLocationSpec("test", "test", new Path(".")).identifier
    TestLocationSpec("test1239", "test1239", new Path(".")).identifier
    TestLocationSpec("012345689", "012345689", new Path(".")).identifier
    TestLocationSpec("0123test", "0123test", new Path(".")).identifier
    // test dataspace
    TestLocationSpec("test", "test", new Path(".")).dataspace
    TestLocationSpec("test1239", "test1239", new Path(".")).dataspace
    TestLocationSpec("012345689", "012345689", new Path(".")).dataspace
    TestLocationSpec("0123test", "0123test", new Path(".")).dataspace
    // test source path
    TestLocationSpec("a", "b", new Path("/tmp/test")).sourcePath should be (new Path("/tmp/test"))
  }

  test("IndexLocationSpec - empty path") {
    val err = intercept[IllegalArgumentException] {
      TestLocationSpec("a", "b", new Path(""))
    }
    assert(err.getMessage.contains("Can not create a Path from an empty string"))
  }

  test("IndexLocationSpec - source spec") {
    val spec = SourceLocationSpec("parquet", new Path("."))
    spec.identifier should be ("parquet")
    spec.dataspace should be ("source")
    spec.sourcePath should be (new Path("."))
  }

  test("IndexLocationSpec - catalog spec") {
    val spec = CatalogLocationSpec("parquet", new Path("."))
    spec.identifier should be ("parquet")
    spec.dataspace should be ("catalog")
    spec.sourcePath should be (new Path("."))
  }

  test("IndexLocationSpec - toString") {
    TestLocationSpec("identifier", "dataspace", new Path("/tmp/test")).toString should be (
      "[dataspace/identifier, source=/tmp/test]")
    SourceLocationSpec("parquet", new Path("/tmp/source")).toString should be (
      "[source/parquet, source=/tmp/source]")
    CatalogLocationSpec("parquet", new Path("/tmp/catalog")).toString should be (
      "[catalog/parquet, source=/tmp/catalog]")
  }
}
