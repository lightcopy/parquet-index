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

import org.apache.spark.sql.{Column, DataFrame}

/**
 * [[Catalog]] provides access to internal index metastore.
 */
abstract class Catalog {

  /**
   * Get fully-qualified path to the backed metastore. Can be local file system or HDFS.
   */
  def metastore: String

  /**
   * List index directories available in metastore. Returns sequence of [[IndexStatus]],
   * which might have different underlying implementation, e.g. Parquet or ORC.
   */
  def listIndexes(): Seq[IndexStatus]

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
   * Query index based on [[IndexSpec]] and condition. Internal behaviour should depend on condition
   * and supported predicates as well as amount of data returned.
   * @param indexSpec partial index specification
   * @param condition filter expression to use
   */
  def queryIndex(indexSpec: IndexSpec, condition: Column): DataFrame

  /**
   * Refresh in-memory cache if supported by catalog.
   * Should invalidate cache and reload from metastore.
   * @param indexSpec partial index specification
   */
  def refreshIndex(indexSpec: IndexSpec): Unit = { }
}
