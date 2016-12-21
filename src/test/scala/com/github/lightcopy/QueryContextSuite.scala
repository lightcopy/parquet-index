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

import org.apache.spark.sql.DataFrameIndexManager
import org.apache.spark.sql.execution.datasources.IndexSourceStrategy

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class QueryContextSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("test implicits and added strategy") {
    import com.github.lightcopy.implicits._
    val manager: DataFrameIndexManager = spark.index
    val strategies = spark.experimental.extraStrategies
    strategies.contains(IndexSourceStrategy) should be (true)
  }

  test("strategy should be added once only") {
    import com.github.lightcopy.implicits._
    for (i <- 0 until 10) {
      val manager = spark.index
      val strategies = spark.experimental.extraStrategies
      strategies.filter(_ == IndexSourceStrategy).length should be (1)
    }
  }
}
