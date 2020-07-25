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

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class TrivialFilterSuite extends UnitTestSuite {
  test("Trivial - convert to true") {
    Trivial(true) should be (AlwaysTrue)
  }

  test("Trivial - convert to false") {
    Trivial(false) should be (AlwaysFalse)
  }

  case class FilterTest(filter: Seq[Filter])

  test("Trivial - unapply Trivial(true)") {
    FilterTest(Trivial(true) :: Nil) match {
      case FilterTest(Trivial(true) :: Nil) => // OK
      case _ => throw new AssertionError("Invalid trivial filter conversion")
    }
  }

  test("Trivial - unapply Trivial(false)") {
    FilterTest(Trivial(false) :: Nil) match {
      case FilterTest(Trivial(false) :: Nil) => // OK
      case _ => throw new AssertionError("Invalid trivial filter conversion")
    }
  }

  test("Trivial - unapply method") {
    Trivial.unapply(AlwaysTrue) should be (Some(true))
    Trivial.unapply(AlwaysFalse) should be (Some(false))
    Trivial.unapply(EqualTo("col", "value")) should be (None)
  }
}
