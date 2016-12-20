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

/**
 * Interface [[MetastoreSupport]] to describe how index metadata should be saved or loaded.
 */
trait MetastoreSupport {
  /**
   * Index path suffix to identify file format to load index.
   * Must be lowercase alpha-numeric characters only.
   * TODO: Fix method to resolve collisions in names between formats
   */
  def identifier: String

  /**
   * Spark FileFormat datasource that is a base for metastore index. For example, for Parquet index
   * file format is `ParquetFileFormat`, that provides necessary reader to return records.
   */
  def fileFormat: FileFormat

  /**
   * Create index based on provided index directory that is guaranteed to exist.
   * @param metastore current index metastore
   * @param indexDirectory index directory of metastore to store relevant data
   * @param tablePath path to the table, mainly for reference, should not be used to list files
   * @param isAppend flag indicates whether or not data should be appended to existing files
   * @param partitionSpec partition spec for table
   * @param partitions all partitions for table (include file status and partition as row)
   * @param columns sequence of columns to index
   */
  def createIndex(
      metastore: Metastore,
      indexDirectory: FileStatus,
      tablePath: FileStatus,
      isAppend: Boolean,
      partitionSpec: PartitionSpec,
      partitions: Seq[Partition],
      columns: Seq[Column]): Unit

  /**
   * Load index into `MetastoreIndexCatalog`, which provides methods to return all files, apply
   * filtering on discovered files and infer schema.
   * @param metastore current index metastore
   * @param indexDirectory index directory of metastore to load relevant data
   */
  def loadIndex(metastore: Metastore, indexDirectory: FileStatus): MetastoreIndexCatalog

  /**
   * Delete index, this method should be overwritten when special handling of data inside index
   * directory is required. Do not delete `indexDirectory` itself, it will be removed after this
   * method is finished.
   * @param metastore current metastore
   * @param indexDirectory index directory of metastore to clean up
   */
  def deleteIndex(metastore: Metastore, indexDirectory: FileStatus): Unit = { }
}
