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

import org.apache.hadoop.fs.FileSystem

import org.apache.spark.sql.{Column, DataFrame}

import com.github.lightcopy.index.Index

/**
 * [[Catalog]] manages internal index metastore. Implementations can store metadata and index data
 * on either disk on external database. Some methods are file-system specific, but usage is
 * enforced to use in file-system related tasks, so database implementations can throw unsupported
 * exception safely.
 */
abstract class Catalog {

  /**
   * File system for catalog, to manage file-system related tasks. If database catalog does not
   * provide this functionality, it should throw unsupported operation exception instead.
   */
  def fs: FileSystem

  /**
   * Get fully resolved metastore location, can be jdbc connection string in case of databases, or
   * qualified path to the file-system backed metastore.
   */
  def metastoreLocation: String

  /**
   * Get fresh index location that should not have collisions with previously created index
   * locations, otherwise should throw exception. For file-system catalog, this should return
   * fresh fully-qualified path to the index directory. For database backed catalog this should
   * return keyspace and table name.
   */
  def getFreshIndexLocation(): String

  /**
   * List index directories available in metastore. Returns sequence of [[Index]],
   * which might have different underlying implementation, e.g. Parquet or ORC.
   */
  def listIndexes(): Seq[Index]

  /**
   * Find index based on provided [[IndexSpec]]. Should return None, if index does not exist, and
   * should fail if index is corrupt.
   * @param indexSpec partial index specification
   */
  def getIndex(indexSpec: IndexSpec): Option[Index]

  /**
   * Create index based on partial information as [[IndexSpec]] and set of columns. Should check if
   * index can be created and apply certain action based on save mode.
   * @param indexSpec partial index specification
   * @param columns list of columns to index
   */
  def createIndex(indexSpec: IndexSpec, columns: Seq[Column]): Unit

  /**
   * Drop index based on [[IndexSpec]], note that if no index matches spec, results in no-op.
   * @param indexSpec partial index specification
   */
  def dropIndex(indexSpec: IndexSpec): Unit

  /**
   * Query index based on [[IndexSpec]] and condition. Internal behaviour should depend on
   * condition and supported predicates as well as amount of data returned. Note that fallback
   * strategy may not be supported by underlying index implementation.
   * @param indexSpec partial index specification
   * @param condition filter expression to use
   */
  def queryIndex(indexSpec: IndexSpec, condition: Column): DataFrame

  /**
   * Refresh in-memory cache if supported by catalog.
   * Should invalidate cache and reload from metastore.
   * @param indexSpec partial index specification
   */
  def refreshIndex(indexSpec: IndexSpec): Unit
}
