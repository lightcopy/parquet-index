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

import java.io.{InputStream, OutputStream}

import org.apache.spark.sql.sources._

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

/** Test in-filter that returns true, if value in provided sequence */
private[parquet] case class TestInFilter(values: Seq[Any]) extends ColumnFilterStatistics {
  override protected def updateFunc: PartialFunction[Any, Unit] = {
    case value => // no-op
  }
  override protected def mightContainFunc: PartialFunction[Any, Boolean] = {
    case value => values.contains(value)
  }
  override protected def needToReadData(): Boolean = false
  override protected def serializeData(out: OutputStream): Unit = { }
  override protected def deserializeData(in: InputStream): Unit = { }
}

private[parquet] case class TestUnsupportedFilter() extends Filter

class ParquetIndexFiltersSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  // Parse provided metadata into dummy block metadata with indexed columns for folding filter
  // only single block metadata is created for provided column metadata
  def parseColumns(columns: ParquetColumnMetadata*): Array[ParquetBlockMetadata] = {
    Array(ParquetBlockMetadata(123, columns.map(col => (col.fieldName, col)).toMap))
  }

  // Create integer statistics with min/max/nulls.
  def intStatistics(min: Int, max: Int, numNulls: Int): IntColumnStatistics = {
    val stats = IntColumnStatistics()
    stats.updateMinMax(min)
    stats.updateMinMax(max)
    for (i <- 0 until numNulls) {
      stats.incrementNumNulls()
    }
    stats
  }

  test("foldFilter - return trivial when EqualTo attribute is not indexed column") {
    val blocks = Array(ParquetBlockMetadata(123, Map.empty))
    ParquetIndexFilters(fs, blocks).foldFilter(EqualTo("a", 1)) should be (Trivial(true))
  }

  test("foldFilter - discard EqualTo when value is not in statistics") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    ParquetIndexFilters(fs, blocks).foldFilter(EqualTo("a", 1)) should be (Trivial(false))
  }

  test("foldFilter - accept EqualTo when value is in statistics and no filter") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    ParquetIndexFilters(fs, blocks).foldFilter(EqualTo("a", 3)) should be (Trivial(true))
  }

  test("foldFilter - discard EqualTo when value is in statistics, but rejected by filter") {
    val blocks = parseColumns(
      ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), Some(TestInFilter(Nil)))
    )
    ParquetIndexFilters(fs, blocks).foldFilter(EqualTo("a", 3)) should be (Trivial(false))
  }

  test("foldFilter - accept EqualTo when value is in statistics, and in filter") {
    val blocks = parseColumns(
      ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), Some(TestInFilter(3 :: Nil)))
    )
    ParquetIndexFilters(fs, blocks).foldFilter(EqualTo("a", 3)) should be (Trivial(true))
  }

  test("foldFilter - reduce all blocks results using Or") {
    val blocks =
      parseColumns(ParquetColumnMetadata("a", 5, intStatistics(2, 4, 0), None)) :+
        ParquetBlockMetadata(1, Map.empty)
    // should return true, because first filter returns Trivial(false), second filter returns
    // Trivial(true), and result is Or(Trivial(true), Trivial(false))
    ParquetIndexFilters(fs, blocks).foldFilter(EqualTo("a", 1)) should be (Trivial(true))
  }

  test("foldFilter - return true for unsupported filter") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = TestUnsupportedFilter()
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - In, no values match") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(4, 5, 0), None))
    val filter = In("a", Array(1, 2, 3))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - In, no values match, filter used") {
    val blocks = parseColumns(
      ParquetColumnMetadata("a", 123, intStatistics(0, 5, 0),
        Some(TestInFilter(0 :: 5 :: Nil)))
    )
    val filter = In("a", Array(1, 2, 3))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - In, some values match") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(1, 2, 0), None))
    ParquetIndexFilters(fs, blocks).
      foldFilter(In("a", Array(1, 2, 3))) should be (Trivial(true))
  }

  test("foldFilter - In, some values match, filter used") {
    val blocks = parseColumns(
      ParquetColumnMetadata("a", 123, intStatistics(1, 5, 0),
        Some(TestInFilter(1 :: 5 :: Nil)))
    )
    val filter = In("a", Array(1, 2, 3))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - IsNull, non-null statistics") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    ParquetIndexFilters(fs, blocks).foldFilter(IsNull("a")) should be (Trivial(false))
  }

  test("foldFilter - IsNull, null statistics") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 1), None))
    ParquetIndexFilters(fs, blocks).foldFilter(IsNull("a")) should be (Trivial(true))
  }

  // Curently this filter is not supported, should always return Trivial(true)
  test("foldFilter - IsNotNull, null statistics") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 1), None))
    ParquetIndexFilters(fs, blocks).foldFilter(IsNotNull("a")) should be (Trivial(true))
  }

  // Curently this filter is not supported, should always return Trivial(true)
  test("foldFilter - IsNotNull, non-null statistics") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    ParquetIndexFilters(fs, blocks).foldFilter(IsNotNull("a")) should be (Trivial(true))
  }

  test("foldFilter - GreaterThan, value is greater than max") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    ParquetIndexFilters(fs, blocks).foldFilter(GreaterThan("a", 5)) should be (Trivial(false))
  }

  test("foldFilter - GreaterThan, value is equal to max") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    ParquetIndexFilters(fs, blocks).foldFilter(GreaterThan("a", 4)) should be (Trivial(false))
  }

  test("foldFilter - GreaterThan, value is less than max") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    ParquetIndexFilters(fs, blocks).foldFilter(GreaterThan("a", 3)) should be (Trivial(true))
  }

  test("foldFilter - GreaterThan, value is less than min") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    ParquetIndexFilters(fs, blocks).foldFilter(GreaterThan("a", 1)) should be (Trivial(true))
  }

  test("foldFilter - GreaterThanOrEqual, value is greater than max") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = GreaterThanOrEqual("a", 5)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - GreaterThanOrEqual, value is equal to max") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = GreaterThanOrEqual("a", 4)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - GreaterThanOrEqual, value is less than max") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = GreaterThanOrEqual("a", 3)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - GreaterThanOrEqual, value is less than min") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = GreaterThanOrEqual("a", 1)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - LessThan, value is less than min") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = LessThan("a", 1)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - LessThan, value is equal to min") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = LessThan("a", 2)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - LessThan, value is greater than min") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = LessThan("a", 3)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - LessThan, value is greater than max") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = LessThan("a", 5)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - LessThanOrEqual, value is less than min") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = LessThanOrEqual("a", 1)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - LessThanOrEqual, value is equal to min") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = LessThanOrEqual("a", 2)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - LessThanOrEqual, value is greater than min") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = LessThanOrEqual("a", 3)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - LessThanOrEqual, value is greater than max") {
    val blocks = parseColumns(ParquetColumnMetadata("a", 123, intStatistics(2, 4, 0), None))
    val filter = LessThanOrEqual("a", 5)
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - And(true, true)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(true), Trivial(true))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - And(true, false)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(true), Trivial(false))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - And(false, true)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(false), Trivial(true))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - And(false, false)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = And(Trivial(false), Trivial(false))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - Or(true, true)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(true), Trivial(true))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Or(true, false)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(true), Trivial(false))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Or(false, true)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(false), Trivial(true))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Or(null, true)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(null, Trivial(true))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Or(false, false)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Or(Trivial(false), Trivial(false))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }

  test("foldFilter - Not(false)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Not(Trivial(false))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(true))
  }

  test("foldFilter - Not(true)") {
    val blocks = Array.empty[ParquetBlockMetadata]
    val filter = Not(Trivial(true))
    ParquetIndexFilters(fs, blocks).foldFilter(filter) should be (Trivial(false))
  }
}
