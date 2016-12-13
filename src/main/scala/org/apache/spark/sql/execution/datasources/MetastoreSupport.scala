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

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.Column

trait MetastoreSupport {
  /**
   * Index path suffix to identify file format to load index.
   * Must be lowercase alpha-numeric characters only.
   */
  def identifier: String

  /**
   * Create index based on provided index directory that is guaranteed to exist.
   * @param metastore current index metastore
   * @param indexDirectory index directory of metastore to store relevant data
   * @param isAppend flag indicates whether or not data should be appended to existing files
   * @param partitionSpec partition spec for table
   * @param files all partitions for table (partition is empty row, if no directory partitioning)
   * @param columns sequence of columns to index
   */
  def createIndex(
      metastore: Metastore,
      indexDirectory: FileStatus,
      isAppend: Boolean,
      partitionSpec: PartitionSpec,
      files: Seq[Partition],
      columns: Seq[Column]): Unit

  /**
   * Delete index, this method should be overwritten when special handling of data inside index
   * directory is required. Do not delete `indexDirectory` itself, it will be removed after this
   * method is finished.
   * @param metastore current metastore
   * @param indexDirectory index directory of metastore to clean up
   */
  def deleteIndex(metastore: Metastore, indexDirectory: FileStatus): Unit = { }
}
