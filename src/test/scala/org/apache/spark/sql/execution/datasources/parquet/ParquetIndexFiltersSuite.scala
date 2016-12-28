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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.sources._

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

/** Test in-filter that returns true, if value in provided sequence */
private[parquet] case class TestInFilter(values: Seq[Any]) extends ParquetColumnFilter {
  override def init(conf: Configuration): Unit = { }
  override def isSet(): Boolean = true
  override def  destroy(): Unit = { }
  override def mightContain(value: Any): Boolean = values.contains(value)
}

private[parquet] case class TestUnsupportedFilter() extends Filter

class ParquetIndexFiltersSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("foldFilter - return trivial when EqualTo attribute is not indexed column") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(ParquetBlockMetadata(123, Map.empty))
    ParquetIndexFilters(conf, blocks).foldFilter(EqualTo("a", 1)) should be (Trivial(true))
  }

  test("foldFilter - discard EqualTo when value is not in statistics") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(EqualTo("a", 1)) should be (Trivial(false))
  }

  test("foldFilter - accept EqualTo when value is in statistics and no filter") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(EqualTo("a", 3)) should be (Trivial(true))
  }

  test("foldFilter - discard EqualTo when value is in statistics, but rejected by filter") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0),
            Some(TestInFilter(Nil)))
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(EqualTo("a", 3)) should be (Trivial(false))
  }

  test("foldFilter - accept EqualTo when value is in statistics, and in filter") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0),
            Some(TestInFilter(3 :: Nil)))
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(EqualTo("a", 3)) should be (Trivial(true))
  }

  test("foldFilter - reduce all blocks results using Or") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      ),
      ParquetBlockMetadata(123, Map.empty)
    )
    // should return true, because first filter returns Trivial(false), second filter returns
    // Trivial(true), and result is Or(Trivial(true), Trivial(false))
    ParquetIndexFilters(conf, blocks).foldFilter(EqualTo("a", 1)) should be (Trivial(true))
  }

  test("foldFilter - return true for unsupported filter") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = TestUnsupportedFilter()
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - In, no values match") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(4, 5, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(In("a", Array(1, 2, 3))) should be (Trivial(false))
  }

  test("foldFilter - In, no values match, filter used") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(0, 5, 0),
            Some(TestInFilter(Array(0, 5))))
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(In("a", Array(1, 2, 3))) should be (Trivial(false))
  }

  test("foldFilter - In, some values match") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(1, 2, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(In("a", Array(1, 2, 3))) should be (Trivial(true))
  }

  test("foldFilter - In, some values match, filter used") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(1, 5, 0),
            Some(TestInFilter(Array(1, 5))))
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(In("a", Array(1, 2, 3))) should be (Trivial(true))
  }

  test("foldFilter - IsNull, non-null statistics") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(IsNull("a")) should be (Trivial(false))
  }

  test("foldFilter - IsNull, null statistics") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 1), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(IsNull("a")) should be (Trivial(true))
  }

  test("foldFilter - IsNotNull, null statistics") {
    // filter always yields true
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 1), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(IsNotNull("a")) should be (Trivial(true))
  }

  test("foldFilter - IsNotNull, non-null statistics") {
    // filter always yields true
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(IsNotNull("a")) should be (Trivial(true))
  }

  test("foldFilter - GreaterThan, return false") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(GreaterThan("a", 5)) should be (Trivial(false))
  }

  test("foldFilter - GreaterThan, return false for value = max") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(GreaterThan("a", 4)) should be (Trivial(false))
  }

  test("foldFilter - GreaterThan, return true") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    ParquetIndexFilters(conf, blocks).foldFilter(GreaterThan("a", 3)) should be (Trivial(true))
  }

  test("foldFilter - GreaterThanOrEqual, return true for value = max") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    val filter = GreaterThanOrEqual("a", 4)
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - GreaterThanOrEqual, return false") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    val filter = GreaterThanOrEqual("a", 3)
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - LessThan, return false") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    val filter = LessThan("a", 2)
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - LessThan, return true") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    val filter = LessThan("a", 3)
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - LessThanOrEqual, return true if value = min") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array(
      ParquetBlockMetadata(123,
        Map(
          "a" -> ParquetColumnMetadata("a", 123, ParquetIntStatistics(2, 4, 0), None)
        )
      )
    )
    val filter = LessThanOrEqual("a", 2)
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - And(true, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(true), Trivial(true))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - And(true, false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(true), Trivial(false))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - And(false, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(false), Trivial(true))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - And(false, false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(false), Trivial(false))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - Or(true, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(true), Trivial(true))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Or(true, false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(true), Trivial(false))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Or(false, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(false), Trivial(true))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Or(null, true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(null, Trivial(true))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Or(false, false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(false), Trivial(false))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - Not(false)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Not(Trivial(false))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Not(true)") {
    val conf = spark.sessionState.newHadoopConf()
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Not(Trivial(true))
    ParquetIndexFilters(conf, blocks).foldFilter(filter) should be (Trivial(false))
  }
}
