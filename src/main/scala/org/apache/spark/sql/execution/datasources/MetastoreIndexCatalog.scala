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

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

abstract class MetastoreIndexCatalog extends FileCatalog {

  /** Fully qualified table path */
  def tablePath(): Path

  /** Returns the specification of the partitions inferred from the data. */
  def partitionSpec(): PartitionSpec

  /** Index schema, used to prune files based on filters for indexed columns */
  def indexSchema(): StructType

  /** Return schema for listed files */
  def dataSchema(): StructType

  /**
   * Return all valid files grouped into partitions that confirm to partition filters and index
   * filters when available.
   * @param filters filters used to prune which partitions are returned
   * @param indexFilters filters used to select files based on provided index
   */
  def listFilesWithIndexSupport(
      filters: Seq[Expression], indexFilters: Seq[Filter]): Seq[Partition]

  /** Returns all the valid files. */
  def allFiles(): Seq[FileStatus]

  /** Returns the list of input paths from which the catalog will get files. */
  def paths: Seq[Path] = Seq(tablePath)

  /**
   * Returns all valid files grouped into partitions when the data is partitioned. If the data is
   * unpartitioned, this will return a single partition with no partition values.
   * @param filters filters used to prune which partitions are returned.
   */
  def listFiles(filters: Seq[Expression]): Seq[Partition] = {
    listFilesWithIndexSupport(filters, Nil)
  }

  /** Refresh the file listing */
  def refresh(): Unit = { }
}
