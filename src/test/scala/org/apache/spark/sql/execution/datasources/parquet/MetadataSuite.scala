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

import org.apache.spark.sql.sources._

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class MetadataSuite extends UnitTestSuite {
  test("ParquetColumnMetadata - withFilter") {
    val meta = ParquetColumnMetadata("field", 100, IntColumnStatistics(), None)
    meta.withFilter(None).filter should be (None)
    meta.withFilter(Some(null)).filter should be (None)
    meta.withFilter(Some(BloomFilterStatistics())).filter.isDefined should be (true)
  }

  test("ParquetFileStatus - numRows for empty blocks") {
    val status = ParquetFileStatus(status = null, "schema", Array.empty)
    status.numRows should be (0)
  }

  test("ParquetFileStatus - numRows for single block") {
    val status = ParquetFileStatus(status = null, "schema",
      Array(ParquetBlockMetadata(123, Map.empty)))
    status.numRows should be (123)
  }

  test("ParquetFileStatus - numRows for non-empty blocks") {
    val status = ParquetFileStatus(status = null, "schema", Array(
      ParquetBlockMetadata(11, Map.empty),
      ParquetBlockMetadata(12, Map.empty),
      ParquetBlockMetadata(13, Map.empty)))
    status.numRows should be (36)
  }
}
