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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

// Test catalog to check internal methods
private[datasources] class TestIndex extends MetastoreIndex {
  override def tablePath(): Path = ???
  override def partitionSchema: StructType = ???
  override def indexSchema: StructType = ???
  override def dataSchema: StructType = ???
  override def setIndexFilters(filters: Seq[Filter]) = { }
  override def indexFilters: Seq[Filter] = ???
  override def listFilesWithIndexSupport(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      indexFilters: Seq[Filter]): Seq[PartitionDirectory] = ???
  override def inputFiles: Array[String] = ???
  override def sizeInBytes: Long = ???
}

class MetastoreIndexSuite extends UnitTestSuite {
  test("provide sequence of path based on table path") {
    val catalog = new TestIndex() {
      override def tablePath(): Path = new Path("test")
    }

    catalog.rootPaths should be (Seq(new Path("test")))
  }

  test("when using listFiles directly supply empty index filter") {
    var indexSeq: Seq[Filter] = null
    var filterSeq: Seq[Expression] = null
    val catalog = new TestIndex() {
      override def listFilesWithIndexSupport(
          partitionFilters: Seq[Expression],
          dataFilters: Seq[Expression],
          indexFilters: Seq[Filter]): Seq[PartitionDirectory] = {
        indexSeq = indexFilters
        filterSeq = partitionFilters
        Seq.empty
      }
    }

    catalog.listFiles(Seq.empty, Seq.empty)
    indexSeq should be (Nil)
    filterSeq should be (Nil)
  }

  test("refresh should be no-op by default") {
    val catalog = new TestIndex()
    catalog.refresh()
  }
}
